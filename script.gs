/************************************************************
 *  Gmail Auto-Labeler by domain (hosts/*) + consistency audit
 *  Версия: V9.1 (UNPROCESSED + audits + split-by-email + verbose + deep-scan fallback)
 *  Требуется Advanced Gmail Service ("Gmail API") включённым.
 ************************************************************/

/**
 * ===================== НАСТРОЙКИ =====================
 */
// Базовая ветка
const PARENT_LABEL = 'hosts';                  // Родительская ветка
const RARE_LABEL   = `${PARENT_LABEL}/_rare_`; // Общий "редкий" ярлык

// Порог домена
const MIN_FILTER_THRESHOLD = 1;                // Домены с ≥ N тредов получают hosts/<domain> + фильтр

// Что считать «необработанным»
const ONLY_UNREAD = false;                     // true — брать только непрочитанные; false — всю почту

// Пакетность и паузы
const PAGE_SIZE = 200;
const SLEEP_MS = 0;
const MAX_THREADS_PER_RUN = 10000;

// Ретро-применение
const APPLY_FILTER_RETROACTIVE_ON_CREATE = true;
const RETRO_APPLY_LIMIT_PER_CREATE = 0;
const RETRO_APPLY_BATCH = 100;
const RETRO_APPLY_SLEEP_MS = 0;

// Фильтры: архивировать ли входящие
const ARCHIVE_NEW = false;

// «Мягкая остановка» по времени
const TIME_BUDGET_MS = 5 * 60 * 1000;
const TIME_STOP_MARGIN_MS = 10 * 1000;

// Хранилище состояния
const USE_SCRIPT_PROPERTIES = false;

// Сбросы перед запуском
const RESET_CHECKPOINT_ON_START = true;
const RESET_DOMAIN_CACHE_ON_START = true;

// ---- Фазовый «оконный» режим (оставляем как есть, но основной раннер — UNPROCESSED) ----
const PHASE_MODE = 'AUTO';
const BACKFILL_START_FROM = '2000-01-01 00:00:00';
const BACKFILL_WINDOW_DAYS = 30;
const INCREMENTAL_WINDOW_DAYS = 7;
const MAX_WINDOWS_PER_RUN = 0;
const CREATE_FILTERS_DURING_BACKFILL = false;

// Прочее
const CHECKPOINT_WRITE_EVERY = 50;
const COUNT_PAGE_SIZE = 100;
const IGNORE_DOMAINS = new Set([]);

// Пространство имён для пропертей
const PROP_NS = 'HOSTS_SWEEP_V9';
const KEY_CP_BACKFILL    = `${PROP_NS}:CP_BACKFILL_EPOCH`;
const KEY_CP_INCREMENTAL = `${PROP_NS}:CP_INCREMENTAL_EPOCH`;

// Учитывать свои адреса, если письмо входящее
const INCLUDE_OWN_ADDR_IF_INCOMING = true;

/** Отчёты */
const REPORT_RECIPIENT = (Session.getActiveUser().getEmail() || '').trim();

/** Текущая «фаза» */
var CURRENT_PHASE = 'UNPROCESSED';

// Домены, где сплитим по полному адресу
const SPLIT_BY_FULL_EMAIL_DOMAINS = new Set(['gmail.com','bk.ru','mail.ru','yandex.ru','hotmail.com','reltio.com','getcourse.ru']);

// Порог для адреса, если домен split
const MIN_ADDRESS_THRESHOLD = MIN_FILTER_THRESHOLD;

// ====== Диагностика ======
const DEBUG_DECISIONS = true;
const DEBUG_MAX_ROWS  = 200;
const DEBUG_FOCUS_DOMAINS = new Set(['1-ofd.ru']); // пусто = логировать все
const DEBUG_FOCUS_EMAILS  = new Set(['echeck@1-ofd.ru']);

const VERBOSE_LOG = true;  // выкл, когда надоест
function vlog(msg){ if (VERBOSE_LOG) try{ Logger.log(msg); }catch(e){} }

// Глубокий фоллбек, если быстрый парсинг не нашёл отправителя
const DEEP_SCAN_ON_MISSING_IDENTITY = true;

/**
 * ===================== ХРАНИЛИЩЕ/ДАТЫ =====================
 */
function store_() {
  return USE_SCRIPT_PROPERTIES ? PropertiesService.getScriptProperties()
                               : PropertiesService.getUserProperties();
}
function toEpochSec(val){
  if (val == null) return null;
  if (/^\d+$/.test(String(val))) return Number(val);
  const d = new Date(val);
  if (isNaN(d.getTime())) throw new Error('Bad date: '+val);
  return Math.floor(d.getTime()/1000);
}
function fmtDateBoth(date){
  if (!date) return '—';
  const tz = Session.getScriptTimeZone();
  const local = Utilities.formatDate(date, tz, "yyyy-MM-dd HH:mm:ss");
  const off   = Utilities.formatDate(date, tz, "Z");
  const utc   = Utilities.formatDate(date, "Etc/UTC", "yyyy-MM-dd HH:mm:ss 'UTC'");
  return `${local} (${tz} ${off}) | ${utc}`;
}
function fmtEpochBoth(epochSec){ return epochSec ? fmtDateBoth(new Date(epochSec*1000)) : '—'; }
function fmtDateForGmailUI(date){
  if (!date) return '—';
  const tz = Session.getScriptTimeZone();
  return Utilities.formatDate(date, tz, "yyyy/MM/dd");
}


/**
 * ===================== ВСПОМОГАТЕЛЬНЫЕ ОБЩИЕ =====================
 */
function safeAddLabel_(thread, label){
  try { thread.addLabel(label); return true; } catch(e){ Logger.log('addLabel error: '+(e&&e.message?e.message:e)); return false; }
}
function safeRemoveLabel_(thread, label){
  try { thread.removeLabel(label); return true; } catch(e){ Logger.log('removeLabel error: '+(e&&e.message?e.message:e)); return false; }
}
function labelPathParts_(name){ return String(name||'').split('/'); }
function makeDomainLabelName_(domain){ return `${PARENT_LABEL}/${domain}`; }
function makeEmailLabelName_(domain,email){ return `${PARENT_LABEL}/${domain}/${email}`; }


/**
 * ===================== ЯРЛЫКИ/ФИЛЬТРЫ =====================
 */
const _labelByNameCache = new Map();
function ensureLabel(name){
  let l = _labelByNameCache.get(name);
  if (l) return l;
  l = GmailApp.getUserLabelByName(name);
  if (!l){ l = GmailApp.createLabel(name); Logger.log('Label created: '+name); }
  _labelByNameCache.set(name, l);
  return l;
}
const _labelIdCache = new Map();
function ensureLabelId_(name){
  const cached = _labelIdCache.get(name);
  if (cached) return cached;

  const maxAttempts = 4;
  for (let attempt=1; attempt<=maxAttempts; attempt++){
    try{
      const res = Gmail.Users.Labels.list('me');
      const found = (res.labels||[]).find(l=>l.name===name);
      if (found && found.id){ _labelIdCache.set(name, found.id); return found.id; }
    }catch(e){}
    try{
      const created = Gmail.Users.Labels.create({ name, labelListVisibility:'labelShow', messageListVisibility:'show' }, 'me');
      if (created && created.id){ Logger.log('Label created (api): '+name); _labelIdCache.set(name, created.id); return created.id; }
    }catch(e){
      const msg = String(e && e.message || e).toLowerCase();
      if (!msg.includes('already exists') && !msg.includes('409')){
        Logger.log('ensureLabelId_ create error: '+msg);
      }
    }
    Utilities.sleep(250*attempt);
  }
  throw new Error('ensureLabelId_: could not get id for label '+name);
}

function ensureFilterForDomain_(domain, labelName) {
  if (CURRENT_PHASE === 'BACKFILL' && !CREATE_FILTERS_DURING_BACKFILL) return false;

  const domainLabelId = ensureLabelId_(labelName);
  const desiredFrom = '@' + domain;

  if (filtersCacheHas_(desiredFrom, domainLabelId)) return false;

  try{
    const existing = Gmail.Users.Settings.Filters.list('me');
    const filters = (existing.filter || []);
    const hasLegacy = filters.some(f =>
      f.criteria && f.criteria.query === `from:(@${domain})` &&
      f.action && (f.action.addLabelIds || []).includes(domainLabelId)
    );
    if (hasLegacy) return false;
  }catch(e){}

  const action = { addLabelIds: [domainLabelId] };
  if (ARCHIVE_NEW) action.removeLabelIds = ['INBOX'];
  Gmail.Users.Settings.Filters.create({ criteria: { from: desiredFrom }, action }, 'me');
  filtersCacheAdd_(desiredFrom, domainLabelId);
  Logger.log('Filter created (FROM): from:@' + domain + ' -> ' + labelName);

  try {
    const hardStopAt = Date.now() + 20 * 1000;
    const doRetro = APPLY_FILTER_RETROACTIVE_ON_CREATE && CURRENT_PHASE !== 'BACKFILL';
    if (doRetro) {
      const parentLbl = ensureLabel(PARENT_LABEL);
      const n = applyDomainLabelToExisting_(domain, labelName, parentLbl, hardStopAt, RETRO_APPLY_LIMIT_PER_CREATE);
      Logger.log(`Retro-applied ${n} threads for ${domain}`);
    }
  } catch (e) { Logger.log('Retro-apply skipped: ' + (e && e.message ? e.message : e)); }
  return true;
}

function ensureFilterForEmail_(email, labelName) {
  if (CURRENT_PHASE === 'BACKFILL' && !CREATE_FILTERS_DURING_BACKFILL) return false;

  const labelId = ensureLabelId_(labelName);

  if (filtersCacheHas_(email, labelId)) return false;

  const action = { addLabelIds: [labelId] };
  if (ARCHIVE_NEW) action.removeLabelIds = ['INBOX'];
  Gmail.Users.Settings.Filters.create({ criteria: { from: email }, action }, 'me');
  filtersCacheAdd_(email, labelId);
  Logger.log('Filter created (FROM): from:' + email + ' -> ' + labelName);

  try {
    const hardStopAt = Date.now() + 20 * 1000;
    const doRetro = APPLY_FILTER_RETROACTIVE_ON_CREATE && CURRENT_PHASE !== 'BACKFILL';
    if (doRetro) {
      const parentLbl = ensureLabel(PARENT_LABEL);
      const domain = email.slice(email.lastIndexOf('@') + 1);
      const domainLbl = ensureLabel(makeDomainLabelName_(domain));
      const n = applyEmailLabelToExisting_(email, labelName, parentLbl, domainLbl, hardStopAt, RETRO_APPLY_LIMIT_PER_CREATE);
      Logger.log(`Retro-applied ${n} threads for ${email}`);
    }
  } catch (e) { Logger.log('Retro-apply (email) skipped: ' + (e && e.message ? e.message : e)); }
  return true;
}


/**
 * ===================== ПАРСИНГ FROM / ОПРЕДЕЛЕНИЕ ИСТОЧНИКА =====================
 */
let _OWN_ADDRS = null;
function getOwnAddressesSet_() {
  if (_OWN_ADDRS) return _OWN_ADDRS;
  const set = new Set();
  const me = (Session.getActiveUser().getEmail() || '').trim().toLowerCase();
  if (me) set.add(me);
  try { (GmailApp.getAliases() || []).forEach(a => a && set.add(String(a).toLowerCase())); } catch(e){}
  try {
    const sendAs = Gmail.Users.Settings.SendAs.list('me');
    (sendAs.sendAs || []).forEach(x => x && x.sendAsEmail && set.add(String(x.sendAsEmail).toLowerCase()));
  } catch(e){}
  _OWN_ADDRS = set;
  return _OWN_ADDRS;
}

function extractEmailAddress_(fromHeader) {
  if (!fromHeader) return null;
  const s = String(fromHeader);

  const m1 = s.match(/<\s*([^<>@\s"']+@[^<>@\s"']+)\s*>/);
  if (m1) return m1[1].toLowerCase();

  const m2 = s.match(/([A-Za-z0-9._%+'-]+@[A-Za-z0-9.-]+\.[A-Za-z0-9-]{2,})/);
  if (m2) return m2[1].toLowerCase();

  const m3 = s.match(/mailto:([A-Za-z0-9._%+'-]+@[A-Za-z0-9.-]+\.[A-Za-z0-9-]{2,})/i);
  if (m3) return m3[1].toLowerCase();

  return null;
}

function extractDomain(fromHeader) {
  const email = extractEmailAddress_(fromHeader);
  if (!email) return null;
  let domain = email.slice(email.lastIndexOf('@') + 1).toLowerCase();
  domain = domain.replace(/\s+/g, '').replace(/\.+$/, '');
  if (!/^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)+$/.test(domain)) {
    return null;
  }
  return domain;
}

function runExtractDomainTests() {
  const cases = [
    { in: 'Подарок и чек <echeck@1-ofd.ru>',                   want: '1-ofd.ru' },
    { in: 'Vodafone <Vodafone@heartbeat.vodafone.com>',        want: 'heartbeat.vodafone.com' },
    { in: '"Яндекс Музыка" <hello@music.yandex.ru>',           want: 'music.yandex.ru' },
    { in: 'Alice <alice@example.co.uk>',                        want: 'example.co.uk' },
    { in: 'no brackets user@sub.1-test.example.com',           want: 'sub.1-test.example.com' },
    { in: '<foo+tag@a-b.c-d.com>',                              want: 'a-b.c-d.com' },
    { in: 'Company, Inc. <billing@xn--d1acj3b.xn--p1ai>',       want: 'xn--d1acj3b.xn--p1ai' },
    { in: 'mailto:support@help.example.org',                    want: 'help.example.org' },
    { in: '"weird" <bad@host..com>',                            want: null },
  ];
  let ok = 0, fail = 0;
  for (const t of cases) {
    const got = extractDomain(t.in);
    const pass = got === t.want;
    Logger.log((pass ? 'OK   ' : 'FAIL ') + JSON.stringify({ in: t.in, got, want: t.want }));
    if (pass) ok++; else fail++;
  }
  Logger.log(`runExtractDomainTests: ok=${ok}, fail=${fail}`);
}

/** Быстрый геттер темы/From последнего сообщения — теперь с несколькими заголовками и фоллбеком */
function _getHeaderFrom_(headers, name){
  if (!headers) return null;
  const h = headers.find(x => x.name === name);
  return h ? h.value : null;
}
function getLastFromFast_(thread) {
  try {
    const tid = thread.getId();
    const resp = Gmail.Users.Threads.get('me', tid, { format: 'metadata', metadataHeaders: ['From','Sender','Return-Path','Reply-To'] });
    const msgs = (resp.messages || []);
    if (!msgs.length) throw new Error('no messages via Threads.get');
    const last = msgs[msgs.length - 1];
    const headers = (last.payload && last.payload.headers) || [];
    const val = _getHeaderFrom_(headers, 'From') || _getHeaderFrom_(headers, 'Sender') ||
                _getHeaderFrom_(headers, 'Return-Path') || _getHeaderFrom_(headers, 'Reply-To');
    if (val) return val;
    throw new Error('no From-like headers on last message');
  } catch (e) {
    try {
      const msgs = thread.getMessages();
      if (!msgs || !msgs.length) return null;
      return msgs[msgs.length - 1].getFrom() || null;
    } catch (e2) { return null; }
  }
}
function getLastSubjectFast_(thread){
  try {
    const tid = thread.getId();
    const resp = Gmail.Users.Threads.get('me', tid, { format: 'metadata', metadataHeaders: ['Subject'] });
    const msgs = (resp.messages || []);
    if (!msgs.length) return null;
    const last = msgs[msgs.length - 1];
    const headers = (last.payload && last.payload.headers) || [];
    const h = headers.find(x => x.name === 'Subject');
    return h ? h.value : null;
  } catch(e){
    try { return thread.getFirstMessageSubject() || null; } catch(e2){ return null; }
  }
}

/** Лучший «входящий» источник (быстрый путь по Threads.get) */
function getBestIncomingIdentityForThread_(thread) {
  const own = getOwnAddressesSet_();
  try {
    const tid = thread.getId();
    const resp = Gmail.Users.Threads.get('me', tid, { format: 'metadata', metadataHeaders: ['From'] });
    const msgs = (resp.messages || []);
    const counts = new Map();
    const lastEmailForDomain = new Map();
    for (const m of msgs) {
      const labels = (m.labelIds || []);
      const isSent   = labels.includes('SENT');
      const isInbox  = labels.includes('INBOX');
      const isIncoming = (!isSent) || isInbox; // входящее (или «сам себе в копии»)
      if (!isIncoming) continue;

      const headers = (m.payload && m.payload.headers) || [];
      const h = headers.find(x => x.name === 'From');
      if (!h || !h.value) continue;

      const email = extractEmailAddress_(h.value);
      if (!email) continue;
      if (!INCLUDE_OWN_ADDR_IF_INCOMING && own.has(email)) continue;

      const domain = extractDomain(h.value);
      if (!domain || IGNORE_DOMAINS.has(domain)) continue;

      counts.set(domain, (counts.get(domain) || 0) + 1);
      lastEmailForDomain.set(domain, email);
    }
    if (!counts.size) return {domain:null, email:null, scanned:msgs.length};
    let best=null, bestCount=-1;
    for (const [d,c] of counts.entries()) if (c>bestCount){ best=d; bestCount=c; }
    const email = lastEmailForDomain.get(best);
    return {domain:best, email, scanned:msgs.length};
  } catch (e) {
    const fromHeader = getLastFromFast_(thread);
    const email = extractEmailAddress_(fromHeader);
    const domain = extractDomain(fromHeader);
    return {domain, email, scanned: fromHeader ? 1 : 0};
  }
}

/** Глубокий фоллбек: читаем реальные сообщения через GmailApp и вытаскиваем From */
function getBestIncomingIdentityForThreadDeep_(thread){
  const own = getOwnAddressesSet_();
  try{
    const msgs = thread.getMessages(); // полные сообщения
    const counts = new Map();
    const lastEmailForDomain = new Map();
    for (const m of msgs){
      let from;
      try { from = m.getFrom(); } catch(e){ from = null; }
      if (!from) continue;

      const email = extractEmailAddress_(from);
      if (!email) continue;
      if (!INCLUDE_OWN_ADDR_IF_INCOMING && own.has(email)) continue;

      const domain = extractDomain(from);
      if (!domain || IGNORE_DOMAINS.has(domain)) continue;

      counts.set(domain, (counts.get(domain) || 0) + 1);
      lastEmailForDomain.set(domain, email);
    }
    if (!counts.size) return {domain:null, email:null, scanned:msgs.length};
    let best=null, bestCount=-1;
    for (const [d,c] of counts.entries()) if (c>bestCount){ best=d; bestCount=c; }
    const email = lastEmailForDomain.get(best);
    return {domain:best, email, scanned:msgs.length};
  } catch(e){
    return {domain:null, email:null, scanned:0};
  }
}


/**
 * ===================== ПОДСЧЁТ/КЕШ ДОМЕНОВ И АДРЕСОВ =====================
 */
function domainCacheKey_(d){ return `${PROP_NS}:DOMAIN_STATUS:${d}`; }
function addressCacheKey_(email){ return `${PROP_NS}:ADDRESS_STATUS:${email}`; }

function countThreadsForDomainUpToThresholdFast_(domain, threshold) {
  let total = 0, pageToken = null;
  const q = `from:(@${domain})`;
  do {
    const batchSize = Math.min(500, Math.max(50, threshold - total));
    const res = Gmail.Users.Threads.list('me', { q, maxResults: batchSize, pageToken });
    const arr = (res.threads || []);
    total += arr.length;
    if (total >= threshold) return { count: total, atLeastThreshold: true };
    pageToken = res.nextPageToken || null;
  } while (pageToken);
  return { count: total, atLeastThreshold: (total >= threshold) };
}
function getDomainStatus_(domain, runLocalCount = 0) {
  const props = store_();
  const key = domainCacheKey_(domain);
  let cached = null;
  const raw = props.getProperty(key);
  if (raw) { try { cached = JSON.parse(raw); } catch (e) {} }

  if (cached && cached.atLeastThreshold === true) return cached;

  const baseCount = (cached && Number.isFinite(cached.count)) ? cached.count : 0;
  const approx = baseCount + (runLocalCount || 0);
  if (approx < MIN_FILTER_THRESHOLD) {
    const out = { count: approx, atLeastThreshold: false, ts: Date.now() };
    props.setProperty(key, JSON.stringify(out));
    return out;
  }

  const res = countThreadsForDomainUpToThresholdFast_(domain, MIN_FILTER_THRESHOLD);
  const toStore = { count: res.count, atLeastThreshold: res.atLeastThreshold, ts: Date.now() };
  props.setProperty(key, JSON.stringify(toStore));
  return toStore;
}

function countThreadsForEmailUpToThresholdFast_(email, threshold) {
  let total = 0, pageToken = null;
  const q = `from:("${email}")`;
  do {
    const batchSize = Math.min(500, Math.max(50, threshold - total));
    const res = Gmail.Users.Threads.list('me', { q, maxResults: batchSize, pageToken });
    const arr = (res.threads || []);
    total += arr.length;
    if (total >= threshold) return { count: total, atLeastThreshold: true };
    pageToken = res.nextPageToken || null;
  } while (pageToken);
  return { count: total, atLeastThreshold: (total >= threshold) };
}
function getAddressStatus_(email, runLocalCount = 0) {
  const props = store_();
  const key = addressCacheKey_(email);
  let cached = null;
  const raw = props.getProperty(key);
  if (raw) { try { cached = JSON.parse(raw); } catch (e) {} }

  if (cached && cached.atLeastThreshold === true) return cached;

  const baseCount = (cached && Number.isFinite(cached.count)) ? cached.count : 0;
  const approx = baseCount + (runLocalCount || 0);
  if (approx < MIN_ADDRESS_THRESHOLD) {
    const out = { count: approx, atLeastThreshold: false, ts: Date.now() };
    props.setProperty(key, JSON.stringify(out));
    return out;
  }

  const res = countThreadsForEmailUpToThresholdFast_(email, MIN_ADDRESS_THRESHOLD);
  const toStore = { count: res.count, atLeastThreshold: res.atLeastThreshold, ts: Date.now() };
  props.setProperty(key, JSON.stringify(toStore));
  return toStore;
}


/**
 * ===================== МИГРАЦИИ / РЕТРО-ПРИМЕНЕНИЕ =====================
 */
function applyDomainLabelToExisting_(domain, labelName, parentLbl, hardStopAt, maxToApply) {
  const lbl = ensureLabel(labelName);
  const query = `from:(@${domain}) -label:"${labelName}"`;
  let applied = 0;
  while (true) {
    if (hardStopAt && Date.now() >= hardStopAt) break;
    const batch = GmailApp.search(query, 0, RETRO_APPLY_BATCH);
    if (!batch.length) break;
    for (const t of batch) {
      if (hardStopAt && Date.now() >= hardStopAt) break;
      safeAddLabel_(t, lbl);
      safeAddLabel_(t, parentLbl);
      applied++;
      if (maxToApply && applied >= maxToApply) break;
    }
    if (maxToApply && applied >= maxToApply) break;
    Utilities.sleep(RETRO_APPLY_SLEEP_MS);
  }
  return applied;
}

function applyEmailLabelToExisting_(email, labelName, parentLbl, domainLbl, hardStopAt, maxToApply) {
  const lbl = ensureLabel(labelName);
  const query = `from:("${email}") -label:"${labelName}"`;
  let applied = 0;
  while (true) {
    if (hardStopAt && Date.now() >= hardStopAt) break;
    const batch = GmailApp.search(query, 0, RETRO_APPLY_BATCH);
    if (!batch.length) break;
    for (const t of batch) {
      if (hardStopAt && Date.now() >= hardStopAt) break;
      safeAddLabel_(t, lbl);
      if (domainLbl) safeAddLabel_(t, domainLbl);
      safeAddLabel_(t, parentLbl);
      applied++;
      if (maxToApply && applied >= maxToApply) break;
    }
    if (maxToApply && applied >= maxToApply) break;
    Utilities.sleep(RETRO_APPLY_SLEEP_MS);
  }
  return applied;
}

function migrateFromRareToNormal_(domain){
  const normalLabelName = makeDomainLabelName_(domain);
  const normalLbl = ensureLabel(normalLabelName);
  const rareLbl = GmailApp.getUserLabelByName(RARE_LABEL);
  const parentLbl = ensureLabel(PARENT_LABEL);
  if (!rareLbl) return 0;
  const threads = GmailApp.search(`from:(@${domain}) label:"${RARE_LABEL}"`);
  let migrated=0;
  for (const t of threads){
    safeRemoveLabel_(t, rareLbl);
    safeAddLabel_(t, normalLbl);
    safeAddLabel_(t, parentLbl);
    migrated++;
  }
  return migrated;
}

function migrateFromRareToEmail_(email, domain){
  const emailLabelName = makeEmailLabelName_(domain, email);
  const emailLbl = ensureLabel(emailLabelName);
  const domainLbl = ensureLabel(makeDomainLabelName_(domain));
  const rareLbl = GmailApp.getUserLabelByName(RARE_LABEL);
  const parentLbl = ensureLabel(PARENT_LABEL);
  if (!rareLbl) return 0;

  const threads = GmailApp.search(`from:("${email}") label:"${RARE_LABEL}"`);
  let migrated=0;
  for (const t of threads){
    safeRemoveLabel_(t, rareLbl);
    safeAddLabel_(t, emailLbl);
    safeAddLabel_(t, domainLbl);
    safeAddLabel_(t, parentLbl);
    migrated++;
  }
  return migrated;
}


/**
 * ===================== АУДИТ КОНСИСТЕНТНОСТИ =====================
 */
function addParentToAllThreadsUnderLabelName_(labelName, hardStopAt) {
  const parentLbl = ensureLabel(PARENT_LABEL);
  const query = `label:"${labelName}" -label:"${PARENT_LABEL}"`;
  let fixed = 0;
  while (true) {
    if (hardStopAt && Date.now() >= hardStopAt) break;
    const batch = GmailApp.search(query, 0, 100);
    if (!batch.length) break;
    for (const t of batch) {
      if (hardStopAt && Date.now() >= hardStopAt) break;
      if (safeAddLabel_(t, parentLbl)) fixed++;
    }
    Utilities.sleep(10);
  }
  return fixed;
}
function auditDomainConsistencyForDomain_(domain, hardStopAt) {
  const labelName = makeDomainLabelName_(domain);
  const parentLbl = ensureLabel(PARENT_LABEL);
  ensureLabel(labelName);

  const created = ensureFilterForDomain_(domain, labelName);
  let retroApplied = 0, rareMigrated = 0, parentsFixed = 0;

  if (!hardStopAt || (Date.now() < hardStopAt)) {
    const localStop = hardStopAt ? Math.min(hardStopAt, Date.now() + 20*1000) : null;
    retroApplied += applyDomainLabelToExisting_(domain, labelName, parentLbl, localStop, 0);
  }
  if (!hardStopAt || (Date.now() < hardStopAt)) rareMigrated += migrateFromRareToNormal_(domain);
  if (!hardStopAt || (Date.now() < hardStopAt)) parentsFixed += addParentToAllThreadsUnderLabelName_(labelName, hardStopAt);

  Logger.log(`AUDIT ${domain}: filterCreated=${!!created}, retroApplied=${retroApplied}, rareRemoved=${rareMigrated}, parentAdded=${parentsFixed}`);
  return { createdFilter: !!created, retroApplied, rareMigrated, parentsFixed };
}
function auditEmailConsistencyForAddress_(email, domain, hardStopAt) {
  const labelName = makeEmailLabelName_(domain, email);
  const parentLbl = ensureLabel(PARENT_LABEL);
  ensureLabel(makeDomainLabelName_(domain));
  ensureLabel(labelName);

  const created = ensureFilterForEmail_(email, labelName);
  let retroApplied = 0, rareMigrated = 0, parentsFixed = 0;

  if (!hardStopAt || (Date.now() < hardStopAt)) {
    const localStop = hardStopAt ? Math.min(hardStopAt, Date.now() + 20*1000) : null;
    const domainLbl = ensureLabel(makeDomainLabelName_(domain));
    retroApplied += applyEmailLabelToExisting_(email, labelName, parentLbl, domainLbl, localStop, 0);
  }
  if (!hardStopAt || (Date.now() < hardStopAt)) rareMigrated += migrateFromRareToEmail_(email, domain);
  if (!hardStopAt || (Date.now() < hardStopAt)) parentsFixed += addParentToAllThreadsUnderLabelName_(labelName, hardStopAt);

  Logger.log(`AUDIT EMAIL ${email}: filterCreated=${!!created}, retroApplied=${retroApplied}, rareRemoved=${rareMigrated}, parentAdded=${parentsFixed}`);
  return { createdFilter: !!created, retroApplied, rareMigrated, parentsFixed };
}


/**
 * ===================== ОТЧЁТ =====================
 */
function sendReportEmail_(s){
  if (!REPORT_RECIPIENT) return;
  const subject = `Gmail hosts sweep [${s.phase}] — ${s.dateISO} (threads ${s.processed})`;
  const startBoth = fmtDateBoth(new Date(s.startMs));
  const endBoth   = fmtDateBoth(new Date(s.endMs));
  const rangeMin  = s.minProcessedEpoch ? fmtEpochBoth(s.minProcessedEpoch) : '—';
  const rangeMax  = s.maxProcessedEpoch ? fmtEpochBoth(s.maxProcessedEpoch) : '—';
  const cpBack    = s.cpBackfill ? fmtEpochBoth(s.cpBackfill) : '—';
  const cpInc     = s.cpIncremental ? fmtEpochBoth(s.cpIncremental) : '—';

  const topRows = s.topDomains.length
    ? s.topDomains.map(([d,c])=>`<tr><td>${d}</td><td style="text-align:right">${c}</td></tr>`).join('')
    : `<tr><td colspan="2" style="text-align:center;color:#888">нет</td></tr>`;

  const decRows = (s.decisions || []).map(r => {
    const safe = x => (x==null?'—':String(x).replace(/&/g,'&amp;').replace(/</g,'&lt;'));
    return `<tr>
      <td>${safe(r.tid)}</td>
      <td>${safe(r.step)}</td>
      <td>${safe(r.from)}</td>
      <td>${safe(r.email)}</td>
      <td>${safe(r.domain)}</td>
      <td>${safe(r.action)}</td>
    </tr>`;
  }).join('');
  const decisionsTable = `
    <h3 style="margin:16px 0 6px">Диагностика решений (первые ${s.decisions ? s.decisions.length : 0})</h3>
    <table border="1" cellpadding="6" style="border-collapse:collapse;min-width:660px">
      <thead>
        <tr><th>Thread ID</th><th>Шаг</th><th>From</th><th>Email</th><th>Domain</th><th>Действие</th></tr>
      </thead>
      <tbody>${decRows || `<tr><td colspan="6" style="text-align:center;color:#888">нет данных</td></tr>`}</tbody>
    </table>`;

  const html = `
  <div style="font-family:system-ui,Segoe UI,Arial,sans-serif">
    <h2 style="margin:0 0 10px">Отчёт автолейблинга (${s.phase})</h2>

    <table style="border-collapse:collapse;min-width:660px;margin-bottom:12px">
      <tbody>
        ${row('Старт запуска', startBoth)}
        ${row('Завершение', endBoth)}
        ${row('Длительность (сек)', s.durationSec)}
        ${row('Остановлено по бюджету', s.stoppedByTimeBudget ? 'да' : 'нет')}
        ${row('Фаза', s.phase)}
        ${row('Окно', s.windowDays ? `${s.windowDays} дн.` : '—')}
        ${row('Режим писем', ONLY_UNREAD ? 'Только непрочитанные' : 'Все письма')}
        ${row('Диапазон писем, фактически охвачен', rangeMin+' → '+rangeMax)}
        ${row('Обработано диалогов', s.processed)}
        ${row('Считано заголовков From', s.messagesProcessed)}
        ${row('Пропущено (уже hosts/*)', s.skippedAlreadyLabeled)}
        ${row('Пропущено (игнор домены/источник)', s.skippedIgnoredDomain)}
        ${row('Помечено rare (общий ярлык)', s.labeledRare)}
        ${row('Помечено normal (hosts/&lt;domain&gt; или hosts/&lt;domain&gt;/&lt;email&gt;)', s.labeledNormal)}
        ${row('Создано новых ярлыков', s.newLabels)}
        ${row('Создано новых фильтров', s.newFilters)}
        ${row('Мигрировано rare→normal/email', s.migratedRareToNormal)}
        ${row('Аудитов inconsistencies', s.auditsTriggered || 0)}
        ${row('Доклеен родитель для уже размеченных', s.parentAddedForExistingChild || 0)}
        ${row('Ошибок', s.errors)}
      </tbody>
    </table>

    <h3 style="margin:16px 0 6px">Чекпойнты</h3>
    <table style="border-collapse:collapse;min-width:660px">
      <tbody>
        ${row('BACKFILL checkpoint', cpBack)}
        ${row('INCREMENTAL checkpoint', cpInc)}
        ${row('Следующий запрос', s.nextQuery ? '<code>'+s.nextQuery+'</code>' : '—')}
        ${row('Подсказка', s.phase==='BACKFILL'
            ? 'Двигаемся ВПЕРЁД окнами; дошли до «сегодня» — переключимся в INCREMENTAL.'
            : (s.phase==='UNPROCESSED'
                ? 'Работаем по выборке: -label:hosts (и is:unread — при включённом ONLY_UNREAD).'
                : 'Инкремент: берём письма новее последнего чекпойнта.'))}
      </tbody>
    </table>

    <h3 style="margin:16px 0 6px">Топ доменов за проход</h3>
    <table border="1" cellpadding="6" style="border-collapse:collapse;min-width:360px">
      <thead><tr><th>Домен</th><th>Диалогов</th></tr></thead>
      <tbody>${topRows}</tbody>
    </table>

    ${decisionsTable}

    <p style="color:#666;font-size:12px;margin-top:16px">
      * Время: <b>локальная зона + UTC</b>.<br/>
      * Фильтры добавляют только доменные/адресные ярлыки (ограничение Gmail). Родительский <b>${PARENT_LABEL}</b> доклеивает скрипт.<br/>
      * Ручной поиск «вне hosts»: <b>${ONLY_UNREAD ? 'is:unread ' : ''}-label:${PARENT_LABEL}</b>.
    </p>
  </div>`;

  GmailApp.sendEmail(REPORT_RECIPIENT, subject, 'HTML report attached', { htmlBody: html });

  function row(name,val){
    return `<tr><td style="padding:6px 8px;border-bottom:1px solid #eee;vertical-align:top">${name}</td>`+
           `<td style="padding:6px 8px;border-bottom:1px solid #eee;text-align:right">${val}</td></tr>`;
  }
}


/**
 * ===================== ОБРАБОТКА ОДНОГО ТРЕДА (ядро) =====================
 */
function pushDecision_(s, row){
  if (!DEBUG_DECISIONS) return;
  if (DEBUG_FOCUS_DOMAINS.size && row.domain && !DEBUG_FOCUS_DOMAINS.has(row.domain)) return;
  if (DEBUG_FOCUS_EMAILS.size  && row.email  && !DEBUG_FOCUS_EMAILS.has(row.email))   return;
  if (!s.decisions) s.decisions = [];
  if (s.decisions.length < DEBUG_MAX_ROWS) s.decisions.push(row);
}

function processSingleThread_(thread, s, parentLbl){
  // диапазон дат
  const d = thread.getLastMessageDate();
  const ts = Math.floor(d.getTime()/1000);
  if (s.minProcessedEpoch===null || ts < s.minProcessedEpoch) s.minProcessedEpoch = ts;
  if (s.maxProcessedEpoch===null || ts > s.maxProcessedEpoch) s.maxProcessedEpoch = ts;

  // быстрые данные для логов
  const lastFromHeader = getLastFromFast_(thread);
  const subj = getLastSubjectFast_(thread);
  const lblNames = thread.getLabels().map(x => x.getName());
  const hasParent = lblNames.includes(PARENT_LABEL);
  const childLabels = lblNames.filter(n => n.startsWith(PARENT_LABEL + '/') && n !== RARE_LABEL);

  vlog(`[THREAD ${thread.getId()}] start: date=${fmtDateForGmailUI(d)}, subject="${subj||'—'}", lastFrom="${lastFromHeader||'—'}", labels=[${lblNames.join(', ')}]`);

  // 1) Если уже есть и parent, и любой child → это уже размеченный тред; просто пропустим
  if (childLabels.length && hasParent) {
    vlog(`[THREAD ${thread.getId()}] already consistent (has parent+child) → skip`);
    s.skippedAlreadyLabeled++;
    s.processed++;
    return;
  }

  // 2) Есть child, НО НЕТ parent → реальный child_present_fix
  if (childLabels.length && !hasParent) {
    vlog(`[THREAD ${thread.getId()}] child_present_fix → run audits for: ${childLabels.join(', ')}`);
    s.auditsTriggered++;
    const perThreadBudgetMs = 10000;
    for (const child of childLabels) {
      const parts = labelPathParts_(child);
      if (parts.length === 3) {
        const domain = parts[1], email = parts[2];
        auditEmailConsistencyForAddress_(email, domain, Date.now() + perThreadBudgetMs);
      } else if (parts.length === 2) {
        const domain = parts[1];
        auditDomainConsistencyForDomain_(domain, Date.now() + perThreadBudgetMs);
      }
    }
    if (safeAddLabel_(thread, parentLbl)) {
      s.parentAddedForExistingChild++;
      vlog(`[THREAD ${thread.getId()}] parent label added`);
    }
    s.processed++;
    return;
  }

  // 3) Полный цикл разметки (как было)
  const who = getBestIncomingIdentityForThread_(thread);
  if (who && Number.isFinite(who.scanned)) s.messagesProcessed += who.scanned;
  const domain = who ? who.domain : null;
  const email  = who ? who.email  : null;

  pushDecision_(s, {
    tid: thread.getId(),
    step: 'parsed',
    from: lastFromHeader,
    email: email || null,
    domain: domain || null,
    action: (domain && email) ? 'ok' : 'missing_identity'
  });

  if (!domain || !email) {
    vlog(`[THREAD ${thread.getId()}] no identity → parent only`);
    s.skippedIgnoredDomain++;
    safeAddLabel_(thread, parentLbl);
    s.processed++;
    return;
  }

  // Split-домены (по полному адресу)
  if (shouldSplitByEmailExact_(domain)) {
    const addrStatus = getAddressStatus_(email, 1);
    const domainLabelName = makeDomainLabelName_(domain);
    ensureLabel(domainLabelName);
    const emailLabelName  = makeEmailLabelName_(domain, email);

    if (addrStatus.atLeastThreshold) {
      let emailLbl = GmailApp.getUserLabelByName(emailLabelName);
      if (!emailLbl) { emailLbl = GmailApp.createLabel(emailLabelName); s.newLabels++; Logger.log('Label created: ' + emailLabelName); }
      safeAddLabel_(thread, emailLbl);
      safeAddLabel_(thread, GmailApp.getUserLabelByName(domainLabelName));
      const created = ensureFilterForEmail_(email, emailLabelName);
      if (created) s.newFilters++;
      const migrated = migrateFromRareToEmail_(email, domain);
      if (migrated>0) s.migratedRareToNormal += migrated;
      s.labeledNormal++;
      vlog(`[THREAD ${thread.getId()}] split_email → label: ${emailLabelName}; filter ensured for ${email}`);
    } else {
      let rareLbl = GmailApp.getUserLabelByName(RARE_LABEL);
      if (!rareLbl) { rareLbl = GmailApp.createLabel(RARE_LABEL); s.newLabels++; Logger.log('Label created: ' + RARE_LABEL); }
      safeAddLabel_(thread, rareLbl);
      s.labeledRare++;
      vlog(`[THREAD ${thread.getId()}] split_email → label: ${RARE_LABEL}`);
    }
    safeAddLabel_(thread, parentLbl);
    s.domainCounts.set(domain, (s.domainCounts.get(domain)||0) + 1);
    s.processed++;
    return;
  }

  // Обычная доменная логика
  s.domainCounts.set(domain, (s.domainCounts.get(domain)||0) + 1);
  const status = getDomainStatus_(domain, s.domainCounts.get(domain) || 0);

  if (status.atLeastThreshold) {
    const normalLabelName = makeDomainLabelName_(domain);
    let lbl = GmailApp.getUserLabelByName(normalLabelName);
    if (!lbl) { lbl = GmailApp.createLabel(normalLabelName); s.newLabels++; Logger.log('Label created: '+normalLabelName); }
    safeAddLabel_(thread, lbl);
    s.labeledNormal++;
    const created = ensureFilterForDomain_(domain, normalLabelName);
    if (created) s.newFilters++;
    const migrated = migrateFromRareToNormal_(domain);
    if (migrated>0) s.migratedRareToNormal += migrated;
    vlog(`[THREAD ${thread.getId()}] domain → label: ${normalLabelName}; filter ensured for @${domain}`);
  } else {
    let rareLbl = GmailApp.getUserLabelByName(RARE_LABEL);
    if (!rareLbl) { rareLbl = GmailApp.createLabel(RARE_LABEL); s.newLabels++; Logger.log('Label created: '+RARE_LABEL); }
    safeAddLabel_(thread, rareLbl);
    s.labeledRare++;
    vlog(`[THREAD ${thread.getId()}] domain → label: ${RARE_LABEL}`);
  }

  safeAddLabel_(thread, parentLbl);
  s.processed++;
  vlog(`[THREAD ${thread.getId()}] done; processed=${s.processed}`);
}

// true — если домен ТОЧНО в списке split-доменов (поддомены не считаем)
function shouldSplitByEmailExact_(domain) {
  return SPLIT_BY_FULL_EMAIL_DOMAINS.has(domain);
}

// ---- Filters cache (from -> Set(labelId))
let _filtersIndex = null;
function primeFiltersCache_(){
  if (_filtersIndex) return;
  _filtersIndex = new Map();
  try{
    const fRes = Gmail.Users.Settings.Filters.list('me');
    const filters = (fRes.filter || []);
    for (const f of filters){
      const from = f.criteria && f.criteria.from;
      const addIds = (f.action && f.action.addLabelIds) || [];
      if (!from || !addIds.length) continue;
      let set = _filtersIndex.get(from);
      if (!set){ set = new Set(); _filtersIndex.set(from, set); }
      for (const id of addIds) set.add(id);
    }
  }catch(e){ Logger.log('primeFiltersCache_ error: ' + (e && e.message ? e.message : e)); }
}
function filtersCacheHas_(from, labelId){
  primeFiltersCache_();
  const set = _filtersIndex.get(from);
  return !!(set && set.has(labelId));
}
function filtersCacheAdd_(from, labelId){
  primeFiltersCache_();
  let set = _filtersIndex.get(from);
  if (!set){ set = new Set(); _filtersIndex.set(from, set); }
  set.add(labelId);
}


/**
 * ============ ОСНОВНОЙ ПРОГОН ПО «НЕОБРАБОТАННЫМ» =============
 */
function runProcessUnprocessedAndReport() {
  CURRENT_PHASE = 'UNPROCESSED';
  if (RESET_DOMAIN_CACHE_ON_START) resetDomainCache();

  const t0 = Date.now(), hardStopAt = t0 + TIME_BUDGET_MS - TIME_STOP_MARGIN_MS;
  const baseQuery = (ONLY_UNREAD ? 'is:unread ' : '') + `-label:${PARENT_LABEL}`;
  const parentLbl = ensureLabel(PARENT_LABEL);

  const s = {
    phase: 'UNPROCESSED',
    windowDays: 0,
    dateISO: new Date().toISOString().slice(0,19).replace('T',' '),
    startMs: t0, endMs: 0, durationSec: 0,
    processed: 0, messagesProcessed: 0,
    skippedAlreadyLabeled: 0, skippedIgnoredDomain: 0,
    labeledRare: 0, labeledNormal: 0, migratedRareToNormal: 0,
    newLabels: 0, newFilters: 0, errors: 0,
    auditsTriggered: 0, parentAddedForExistingChild: 0,
    domainCounts: new Map(), topDomains: [],
    minProcessedEpoch: null, maxProcessedEpoch: null,
    cpBackfill: null, cpIncremental: null,
    stoppedByTimeBudget: false,
    nextQuery: baseQuery,
    decisions: []
  };

  vlog(`[RUN] start UNPROCESSED; query="${baseQuery}", PAGE_SIZE=${PAGE_SIZE}, ONLY_UNREAD=${ONLY_UNREAD}`);

  while (true) {
    if (Date.now() >= hardStopAt) { s.stoppedByTimeBudget = true; break; }
    const batch = GmailApp.search(baseQuery, 0, PAGE_SIZE);
    vlog(`[RUN] fetched batch: ${batch.length} threads`);
    if (!batch.length) break;

    for (const thread of batch) {
      if (Date.now() >= hardStopAt) { s.stoppedByTimeBudget = true; break; }
      try {
        processSingleThread_(thread, s, parentLbl);
        if (s.processed % 20 === 0) vlog(`[RUN] progress: processed=${s.processed}`);
        if (s.processed >= MAX_THREADS_PER_RUN) { s.stoppedByTimeBudget = true; break; }
      } catch (e) {
        Logger.log('ERROR thread: '+(e && e.message ? e.message : e));
        s.errors++;
      }
    }

    if (s.stoppedByTimeBudget) break;
    Utilities.sleep(SLEEP_MS);
  }

  const t1 = Date.now();
  s.endMs = t1;
  s.durationSec = Math.round((t1 - t0)/1000);
  s.topDomains = Array.from(s.domainCounts.entries()).sort((a,b)=>b[1]-a[1]).slice(0,15);
  vlog(`[RUN] done in ${s.durationSec}s; processed=${s.processed}, labeledNormal=${s.labeledNormal}, labeledRare=${s.labeledRare}, newLabels=${s.newLabels}, newFilters=${s.newFilters}, errors=${s.errors}`);
  sendReportEmail_(s);
}

/**
 * ===================== АУДИТЫ / БАТЧ-ДОРАЗМЕТКА =====================
 */
function ensureFiltersForAllHostLabels() {
  const res = Gmail.Users.Labels.list('me');
  const labels = (res.labels || []).filter(l => l.name && l.name.startsWith(PARENT_LABEL + '/') && l.name !== RARE_LABEL);
  if (!labels.length) { Logger.log('No host/* labels found'); return; }

  const fRes = Gmail.Users.Settings.Filters.list('me');
  const filters = (fRes.filter || []);

  // индекс уже существующих (from, labelId)
  const have = new Set();
  for (const f of filters) {
    const from = f.criteria && f.criteria.from;
    const adds = (f.action && f.action.addLabelIds) || [];
    if (from && adds.length) for (const id of adds) have.add(from + '|' + id);
  }

  let created = 0, skipped = 0;
  for (const l of labels) {
    const parts = labelPathParts_(l.name);
    if (parts.length === 3) {
      const email = parts[2];
      const key = email + '|' + l.id;
      if (have.has(key)) { skipped++; continue; }
      const action = { addLabelIds: [l.id] };
      if (ARCHIVE_NEW) action.removeLabelIds = ['INBOX'];
      Gmail.Users.Settings.Filters.create({ criteria: { from: email }, action }, 'me');
      created++;
      Logger.log('Filter created (batch email): ' + email + ' -> ' + l.name);
    } else if (parts.length === 2) {
      const domain = parts[1];
      const key = ('@' + domain) + '|' + l.id;
      if (have.has(key)) { skipped++; continue; }
      const action = { addLabelIds: [l.id] };
      if (ARCHIVE_NEW) action.removeLabelIds = ['INBOX'];
      Gmail.Users.Settings.Filters.create({ criteria: { from: '@' + domain }, action }, 'me');
      created++;
      Logger.log('Filter created (batch domain): @' + domain + ' -> ' + l.name);
    } else { skipped++; }
  }
  Logger.log(`ensureFiltersForAllHostLabels: created=${created}, skipped=${skipped}`);
}

/** как в UI → Apply to matching */
function retroApplyAllHostLabels() {
  const t0 = Date.now(), hardStopAt = t0 + TIME_BUDGET_MS - TIME_STOP_MARGIN_MS;
  const parentLbl = ensureLabel(PARENT_LABEL);

  const names = GmailApp.getUserLabels().map(l => l.getName())
    .filter(n => n.startsWith(PARENT_LABEL + '/') && n !== RARE_LABEL);

  let totalApplied = 0;
  for (const name of names) {
    if (Date.now() >= hardStopAt) break;
    const parts = labelPathParts_(name);
    if (parts.length === 3) {
      const domain = parts[1], email  = parts[2];
      const domainLbl = ensureLabel(makeDomainLabelName_(domain));
      const n = applyEmailLabelToExisting_(email, name, parentLbl, domainLbl, hardStopAt, 0);
      totalApplied += n;
      Logger.log(`Retro-applied ${n} for ${email}`);
    } else if (parts.length === 2) {
      const domain = parts[1];
      const n = applyDomainLabelToExisting_(domain, name, parentLbl, hardStopAt, 0);
      totalApplied += n;
      Logger.log(`Retro-applied ${n} for ${domain}`);
    }
  }
  Logger.log(`Retro-apply done, total applied=${totalApplied}`);
}

function removeRareWhereNormalPresent() {
  const rare = GmailApp.getUserLabelByName(RARE_LABEL);
  if (!rare) { Logger.log('No rare label.'); return; }
  const children = GmailApp.getUserLabels().filter(x => x.getName().startsWith(PARENT_LABEL + '/') && x.getName() !== RARE_LABEL);
  let fixed = 0;
  for (const child of children) {
    let start = 0, batch;
    do {
      batch = GmailApp.search(`label:"${child.getName()}" label:"${RARE_LABEL}"`, start, 100);
      if (!batch.length) break;
      for (const t of batch) { safeRemoveLabel_(t, rare); fixed++; }
      start += batch.length;
      Utilities.sleep(10);
    } while (batch.length === 100);
  }
  Logger.log('removeRareWhereNormalPresent: removed=' + fixed);
}

/** Алиас: доклеить родителя везде, где есть дети */
function auditAndFixHostsParent() { backfillParentHostsLabel(); }

/** Аудит ТОЛЬКО по split-доменам: пройтись по уже созданным hosts/<domain>/<email> */
function auditSplitExistingEmailLabels() {
  const t0 = Date.now(), hardStopAt = t0 + TIME_BUDGET_MS - TIME_STOP_MARGIN_MS;

  // Собираем все ярлыки вида hosts/<domain>/<email>, где <domain> ∈ SPLIT_BY_FULL_EMAIL_DOMAINS
  const names = GmailApp.getUserLabels()
    .map(l => l.getName())
    .filter(n => {
      const parts = labelPathParts_(n);
      return parts.length === 3 &&
             parts[0] === PARENT_LABEL &&
             SPLIT_BY_FULL_EMAIL_DOMAINS.has(parts[1]);
    });

  let audited = 0;
  for (const name of names) {
    if (Date.now() >= hardStopAt) break;
    const [_, domain, email] = labelPathParts_(name);
    try {
      auditEmailConsistencyForAddress_(email, domain, hardStopAt);
      audited++;
    } catch (e) {
      Logger.log('auditSplitExistingEmailLabels error for '+name+': ' + (e && e.message ? e.message : e));
    }
  }
  Logger.log(`auditSplitExistingEmailLabels: audited=${audited}, totalFound=${names.length}`);
}

/** Аудит по конкретному split-домену: пройтись только по его hosts/<domain>/<email> */
function auditSplitExistingEmailLabelsForDomain(domain) {
  if (!SPLIT_BY_FULL_EMAIL_DOMAINS.has(domain)) {
    Logger.log(`Domain ${domain} is not in SPLIT_BY_FULL_EMAIL_DOMAINS — skipped.`);
    return;
  }
  const t0 = Date.now(), hardStopAt = t0 + TIME_BUDGET_MS - TIME_STOP_MARGIN_MS;

  const prefix = `${PARENT_LABEL}/${domain}/`;
  const names = GmailApp.getUserLabels()
    .map(l => l.getName())
    .filter(n => n.startsWith(prefix) && labelPathParts_(n).length === 3);

  let audited = 0;
  for (const name of names) {
    if (Date.now() >= hardStopAt) break;
    const email = labelPathParts_(name)[2];
    try {
      auditEmailConsistencyForAddress_(email, domain, hardStopAt);
      audited++;
    } catch (e) {
      Logger.log('auditSplitExistingEmailLabelsForDomain error for '+name+': ' + (e && e.message ? e.message : e));
    }
  }
  Logger.log(`auditSplitExistingEmailLabelsForDomain(${domain}): audited=${audited}, totalFound=${names.length}`);
}

/** Простой сплиттер пути ярлыка (без ожиданий, что labelPathParts_ уже есть) */
function splitLabelPath_(name){ return String(name||'').split('/'); }

/** Есть ли у треда под данным доменом хоть какой-то вложенный email-лейбл? */
function threadHasAnyEmailChildLabelUnderDomain_(thread, domain) {
  const prefix = `${PARENT_LABEL}/${domain}/`;
  const names = thread.getLabels().map(l => l.getName());
  return names.some(n => n.startsWith(prefix));
}

/**
 * Обнаружить «частые» адреса под split-доменом и создать для них лейблы/фильтры (+ретро)
 */
function auditSplitDiscoverAndCreateForDomain(domain, maxCreatePerRun) {
  if (!SPLIT_BY_FULL_EMAIL_DOMAINS.has(domain)) {
    Logger.log(`Domain ${domain} is not in SPLIT_BY_FULL_EMAIL_DOMAINS — skipped.`);
    return;
  }

  const t0 = Date.now();
  const hardStopAt = t0 + TIME_BUDGET_MS - TIME_STOP_MARGIN_MS;

  const parentLbl = ensureLabel(PARENT_LABEL);
  const domainLabelName = `${PARENT_LABEL}/${domain}`;
  const domainLbl = ensureLabel(domainLabelName);

  const existingEmailLabels = new Set(
    GmailApp.getUserLabels()
      .map(l => l.getName())
      .filter(n => n.startsWith(domainLabelName + '/'))
      .map(n => n.slice(domainLabelName.length + 1))
  );

  let createdEmails = 0;
  let scannedThreads = 0;
  let consideredEmails = 0;
  let skippedAlreadyChild = 0;

  let pageToken = null;
  const baseQuery = `label:"${domainLabelName}"`;
  const counts = new Map();

  outer:
  while (true) {
    if (Date.now() >= hardStopAt) break;

    const res = Gmail.Users.Threads.list('me', { q: baseQuery, maxResults: 200, pageToken });
    const refs = res.threads || [];
    if (!refs.length) break;

    for (const ref of refs) {
      if (Date.now() >= hardStopAt) break outer;

      let thr;
      try { thr = GmailApp.getThreadById(ref.id); } catch (e) { continue; }
      scannedThreads++;

      if (threadHasAnyEmailChildLabelUnderDomain_(thr, domain)) {
        skippedAlreadyChild++;
        continue;
      }

      const who = getBestIncomingIdentityForThread_(thr);
      const email = who && who.email;
      const dom   = who && who.domain;
      if (!email || dom !== domain) continue;

      if (existingEmailLabels.has(email)) continue;

      const c = (counts.get(email) || 0) + 1;
      counts.set(email, c);
      if (c === 1) consideredEmails++;

      if (c >= MIN_ADDRESS_THRESHOLD) {
        const emailLabelName = `${domainLabelName}/${email}`;

        ensureLabel(emailLabelName);
        const createdFilter = ensureFilterForEmail_(email, emailLabelName);
        if (createdFilter) Logger.log(`Filter created for ${email} -> ${emailLabelName}`);

        applyEmailLabelToExisting_(email, emailLabelName, parentLbl, domainLbl, hardStopAt, 0);
        migrateFromRareToEmail_(email, domain);

        existingEmailLabels.add(email);
        createdEmails++;

        if (maxCreatePerRun && createdEmails >= maxCreatePerRun) break outer;
      }
    }

    pageToken = res.nextPageToken || null;
    if (!pageToken) break;
  }

  Logger.log(
    `auditSplitDiscoverAndCreateForDomain(${domain}): ` +
    `created=${createdEmails}, scannedThreads=${scannedThreads}, ` +
    `consideredEmails=${consideredEmails}, skippedAlreadyEmailChild=${skippedAlreadyChild}`
  );
}


/** Пройтись по всем split-доменам и создать недостающие вложенные email-ярлыки/фильтры */
function auditSplitDiscoverAndCreateAll(maxCreatePerDomain) {
  for (const domain of SPLIT_BY_FULL_EMAIL_DOMAINS) {
    auditSplitDiscoverAndCreateForDomain(domain, maxCreatePerDomain || 0);
  }
  Logger.log('auditSplitDiscoverAndCreateAll: done');
}

/** Уже существующие вложенные email-ярлыки: проверить, что фильтры есть, ретро применён и т.п. */
function auditSplitExistingEmailLabels() {
  const t0 = Date.now(), hardStopAt = t0 + TIME_BUDGET_MS - TIME_STOP_MARGIN_MS;

  const names = GmailApp.getUserLabels()
    .map(l => l.getName())
    .filter(n => {
      const parts = splitLabelPath_(n);
      return parts.length === 3 &&
             parts[0] === PARENT_LABEL &&
             SPLIT_BY_FULL_EMAIL_DOMAINS.has(parts[1]);
    });

  let audited = 0;
  for (const name of names) {
    if (Date.now() >= hardStopAt) break;
    const [_, domain, email] = splitLabelPath_(name);
    try {
      auditEmailConsistencyForAddress_(email, domain, hardStopAt);
      audited++;
    } catch (e) {
      Logger.log('auditSplitExistingEmailLabels error for '+name+': ' + (e && e.message ? e.message : e));
    }
  }
  Logger.log(`auditSplitExistingEmailLabels: audited=${audited}, totalFound=${names.length}`);
}

/** Полный «плюс-сплит» аудит — опционально */
function auditFixAllPlusSplit() {
  const t0 = Date.now();
  auditSplitDiscoverAndCreateAll(0);
  auditFixAll();
  auditSplitExistingEmailLabels();
  Logger.log('auditFixAllPlusSplit done in ' + Math.round((Date.now()-t0)/1000) + 's');
}

/** Полный аудит-оркестр — опционально */
function auditFixAll() {
  const t0 = Date.now();
  ensureFiltersForAllHostLabels();
  retroApplyAllHostLabels();
  removeRareWhereNormalPresent();
  auditAndFixHostsParent();
  Logger.log('Audit done in ' + Math.round((Date.now()-t0)/1000) + 's');
}


/**
 * ===================== ЧИСТКА / РЕЗЕТ =====================
 */
function getAllHostLabels_() {
  const all = GmailApp.getUserLabels();
  return all.filter(l => {
    const n = l.getName();
    return n === PARENT_LABEL || n.startsWith(PARENT_LABEL + '/');
  });
}

/** (1) Снять ярлыки hosts и hosts/* со всех тредов */
function removeHostsLabelsFromAllThreads() {
  const labels = getAllHostLabels_();
  if (!labels.length) { Logger.log('No host labels found.'); return; }
  let totalThreads = 0, totalRemovedPairs = 0;
  for (const lab of labels) {
    let removedForThis = 0, loops = 0;
    while (true) {
      const batch = lab.getThreads(0, 100); // всегда offset=0: список укорачивается
      if (!batch.length) break;
      for (const t of batch) { safeRemoveLabel_(t, lab); removedForThis++; }
      totalThreads += batch.length;
      Utilities.sleep(50);
      if (++loops > 20000) break;
    }
    totalRemovedPairs += removedForThis;
    Logger.log(`Label "${lab.getName()}": removed from ${removedForThis} threads`);
  }
  Logger.log(`DONE: processed threads (approx): ${totalThreads}, label removals: ${totalRemovedPairs}`);
}

/** (2) Удалить сами ярлыки hosts и hosts/* из системы */
function deleteHostsLabelsInSystem() {
  const labels = getAllHostLabels_().sort((a,b) => b.getName().length - a.getName().length); // сначала дети
  if (!labels.length) { Logger.log('No host labels to delete.'); return; }
  let deleted = 0;
  for (const lab of labels) {
    try { lab.deleteLabel(); deleted++; Logger.log('Label deleted: ' + lab.getName()); }
    catch (e) { Logger.log('Delete label failed: ' + lab.getName() + ' — ' + (e && e.message ? e.message : e)); }
    Utilities.sleep(20);
  }
  Logger.log(`DONE: deleted ${deleted} labels under "${PARENT_LABEL}"`);
}

/** (3) Удалить фильтры, которые добавляют ярлыки из ветки hosts */
function deleteHostsFilters() {
  const labRes = Gmail.Users.Labels.list('me');
  const hostLabelIds = new Set();
  for (const l of (labRes.labels || [])) {
    if (l.name === PARENT_LABEL || l.name.startsWith(PARENT_LABEL + '/')) hostLabelIds.add(l.id);
  }
  if (hostLabelIds.size === 0) Logger.log('No host label IDs found. (Если ярлыки уже удалены, запускай этот шаг ДО их удаления.)');

  const fRes = Gmail.Users.Settings.Filters.list('me');
  const filters = (fRes.filter || []);
  let deleted = 0, skipped = 0;

  for (const f of filters) {
    const addIds = (f.action && f.action.addLabelIds) || [];
    const touchesHosts = addIds.some(id => hostLabelIds.has(id));
    if (touchesHosts) {
      try { Gmail.Users.Settings.Filters.delete('me', f.id); deleted++; Logger.log('Filter deleted: ' + JSON.stringify(f.criteria || {})); }
      catch (e) { Logger.log('Delete filter failed: ' + (e && e.message ? e.message : e)); }
    } else { skipped++; }
  }
  Logger.log(`DONE: filters deleted=${deleted}, skipped=${skipped}`);
}

/** (4) Полный ресет ветки hosts */
function wipeHostsEverything() {
  Logger.log('Step 1/4: delete filters…');            deleteHostsFilters();
  Logger.log('Step 2/4: remove labels from threads…'); removeHostsLabelsFromAllThreads();
  Logger.log('Step 3/4: delete labels themselves…');  deleteHostsLabelsInSystem();
  Logger.log('Step 4/4: reset checkpoints & cache…');
  try { resetCheckpoints(); } catch (e) { Logger.log('resetCheckpoints error: ' + (e && e.message ? e.message : e)); }
  try { resetDomainCache(); } catch (e) { Logger.log('resetDomainCache error: ' + (e && e.message ? e.message : e)); }
  Logger.log('ALL DONE: hosts system fully reset. Now you can rerun from scratch.');
}


/**
 * ===================== ПРОСМОТР/ЧЕКПОЙНТЫ/КЕШ =====================
 */
function backfillParentHostsLabel(){
  const parent = ensureLabel(PARENT_LABEL);
  const children = GmailApp.getUserLabels().filter(l => l.getName().startsWith(PARENT_LABEL + '/'));
  for (const child of children){
    while (true){
      const batch = GmailApp.search(`label:"${child.getName()}" -label:"${PARENT_LABEL}"`, 0, 100);
      if (!batch.length) break;
      for (const t of batch){ safeAddLabel_(t, parent); }
      Utilities.sleep(20);
    }
  }
}
function listAllProps(){
  const props = store_().getProperties();
  const pretty = JSON.stringify(props, null, 2);
  Logger.log(pretty);
  try{ GmailApp.sendEmail(Session.getActiveUser().getEmail(), 'Props dump', pretty); }catch(e){}
}
function resetCheckpoints(){
  const p = store_();
  p.deleteProperty(KEY_CP_BACKFILL);
  p.deleteProperty(KEY_CP_INCREMENTAL);
  Logger.log('Checkpoints cleared');
}
function resetDomainCache(){
  const p = store_();
  const all = p.getProperties();
  const prefixes = [`${PROP_NS}:DOMAIN_STATUS:`, `${PROP_NS}:ADDRESS_STATUS:`];
  let n=0;
  Object.keys(all).forEach(k=>{
    if (prefixes.some(pref => k.startsWith(pref))){ p.deleteProperty(k); n++; }
  });
  Logger.log(`Cache entries removed: ${n}`);
}
function resetAllNamespaceProps(){
  const p = store_();
  const all = p.getProperties();
  let n=0;
  Object.keys(all).forEach(k=>{ if (k.startsWith(`${PROP_NS}:`)){ p.deleteProperty(k); n++; } });
  Logger.log(`All ${PROP_NS} props removed: ${n}`);
}
function resetScriptParams(){
  Logger.log('Step 1/3: resetCheckpoints …'); resetCheckpoints();
  Logger.log('Step 2/3: resetDomainCache …'); resetDomainCache();
  Logger.log('Step 3/3: resetAllNamespaceProps …'); resetAllNamespaceProps();
  Logger.log('ALL DONE: Now you can rerun from scratch.');
}
