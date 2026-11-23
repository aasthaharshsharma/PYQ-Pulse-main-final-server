'use strict';

/*
|--------------------------------------------------------------------------
| Push notifications (Firebase Cloud Messaging)
|--------------------------------------------------------------------------
| Setup on the server:
|   1. npm install firebase-admin
|   2. Set ONE of these environment variables:
|        FIREBASE_SERVICE_ACCOUNT_JSON  = the full service-account JSON (one line)
|        FIREBASE_SERVICE_ACCOUNT_PATH  = path to the service-account JSON file
|
| Without these the rest of the server still runs; sending reports a clear error.
| Scheduled notifications are sent by a small timer started from server.js.
*/

const path = require('path');

const CHANNEL_ID = 'pyq_pulse_general'; // must match the Android channel in the app
const BATCH_SIZE = 500; // FCM multicast limit per request
const DEAD_TOKEN_CODES = new Set([
  'messaging/registration-token-not-registered',
  'messaging/invalid-registration-token',
  'messaging/invalid-argument',
]);

let messaging = null;
let initError = null;

function initFirebase() {
  if (messaging || initError) return messaging;

  let firebaseAdmin;
  try {
    firebaseAdmin = require('firebase-admin');
  } catch (e) {
    initError = 'firebase-admin is not installed on the server. Run: npm install firebase-admin';
    return null;
  }

  try {
    const json = process.env.FIREBASE_SERVICE_ACCOUNT_JSON || '';
    const filePath = process.env.FIREBASE_SERVICE_ACCOUNT_PATH || '';
    let creds = null;

    if (json) {
      creds = JSON.parse(json);
    } else if (filePath) {
      creds = require(path.resolve(filePath));
    } else {
      initError = 'FIREBASE_SERVICE_ACCOUNT_JSON is not set on the server.';
      return null;
    }

    if (!firebaseAdmin.apps.length) {
      firebaseAdmin.initializeApp({
        credential: firebaseAdmin.credential.cert(creds),
      });
    }

    messaging = firebaseAdmin.messaging();
    return messaging;
  } catch (e) {
    initError = `Firebase setup failed: ${e.message}`;
    return null;
  }
}

function pushStatus() {
  const m = initFirebase();
  return { configured: Boolean(m), error: m ? null : initError };
}

// Android gets a DATA-only message: the app draws the notification itself, so it can use the
// PYQ Pulse logo as the large icon. iOS gets an APNs alert, which the system shows on its own.
function buildMessage({ title, body, route, notificationId }) {
  const data = { title: String(title || ''), body: String(body || '') };
  if (route) data.route = String(route);
  if (notificationId) data.notificationId = String(notificationId);

  return {
    data,
    android: { priority: 'high' },
    apns: {
      headers: { 'apns-priority': '10' },
      payload: { aps: { alert: { title: String(title || ''), body: String(body || '') }, sound: 'default' } },
    },
  };
}

async function fetchActiveTokens(sb, userId = null) {
  const tokens = [];
  const pageSize = 1000;
  for (let from = 0; ; from += pageSize) {
    let query = sb
      .from('device_tokens')
      .select('token')
      .eq('active', true)
      .range(from, from + pageSize - 1);
    if (userId) query = query.eq('user_id', userId);

    const { data, error } = await query;
    if (error) throw new Error(error.message);
    for (const row of data || []) tokens.push(row.token);
    if (!data || data.length < pageSize) break;
  }
  return tokens;
}

async function deactivateTokens(sb, tokens) {
  if (!tokens.length) return;
  await sb
    .from('device_tokens')
    .update({ active: false, updated_at: new Date().toISOString() })
    .in('token', tokens);
}

// Sends one notification row. Returns counts. Throws if Firebase is not set up.
async function dispatch(sb, row) {
  const m = initFirebase();
  if (!m) throw new Error(initError || 'Firebase is not configured.');

  const message = buildMessage({
    title: row.title,
    body: row.body,
    route: row.route,
    notificationId: row.id,
  });

  if (row.target_type === 'topic') {
    await m.send({ ...message, topic: row.target_value || 'all' });
    return { successCount: 1, failureCount: 0 };
  }

  const userId = row.target_type === 'user' ? row.target_value : null;
  if (row.target_type === 'user' && !userId) {
    throw new Error('A user target needs a user id.');
  }

  const tokens = await fetchActiveTokens(sb, userId);
  if (!tokens.length) {
    return { successCount: 0, failureCount: 0 };
  }

  let successCount = 0;
  let failureCount = 0;
  const dead = [];

  for (let i = 0; i < tokens.length; i += BATCH_SIZE) {
    const chunk = tokens.slice(i, i + BATCH_SIZE);
    const result = await m.sendEachForMulticast({ tokens: chunk, ...message });
    successCount += result.successCount;
    failureCount += result.failureCount;

    result.responses.forEach((r, idx) => {
      if (!r.success && DEAD_TOKEN_CODES.has(r.error?.code)) {
        dead.push(chunk[idx]);
      }
    });
  }

  await deactivateTokens(sb, dead);
  return { successCount, failureCount };
}

// Claims a scheduled row (so two timers never send it twice) and sends it.
async function claimAndSend(sb, notificationId) {
  const { data: claimed, error: claimError } = await sb
    .from('notifications')
    .update({ status: 'sending', updated_at: new Date().toISOString() })
    .eq('id', notificationId)
    .eq('status', 'scheduled')
    .select('*')
    .maybeSingle();

  if (claimError) throw new Error(claimError.message);
  if (!claimed) return null;

  try {
    const counts = await dispatch(sb, claimed);
    const { data, error } = await sb
      .from('notifications')
      .update({
        status: 'sent',
        sent_at: new Date().toISOString(),
        success_count: counts.successCount,
        failure_count: counts.failureCount,
        error_message: null,
        updated_at: new Date().toISOString(),
      })
      .eq('id', notificationId)
      .select('*')
      .single();
    if (error) throw new Error(error.message);
    return data;
  } catch (e) {
    await sb
      .from('notifications')
      .update({
        status: 'failed',
        error_message: String(e.message || 'Send failed.').slice(0, 500),
        updated_at: new Date().toISOString(),
      })
      .eq('id', notificationId);
    throw e;
  }
}

// Timer: sends scheduled notifications whose time has come.
function startScheduler(sb) {
  let running = false;

  const tick = async () => {
    if (running) return;
    running = true;
    try {
      const { data, error } = await sb
        .from('notifications')
        .select('id')
        .eq('status', 'scheduled')
        .lte('scheduled_at', new Date().toISOString())
        .limit(20);

      if (error) {
        console.error('Notification scheduler query failed:', error.message);
        return;
      }

      for (const row of data || []) {
        try {
          await claimAndSend(sb, row.id);
          console.log(`Scheduled notification ${row.id} sent.`);
        } catch (e) {
          console.error(`Scheduled notification ${row.id} failed:`, e.message);
        }
      }
    } catch (e) {
      console.error('Notification scheduler error:', e.message);
    } finally {
      running = false;
    }
  };

  const timer = setInterval(tick, 30000);
  timer.unref?.();
  const first = setTimeout(tick, 5000);
  first.unref?.();
  return timer;
}

module.exports = {
  pushStatus,
  claimAndSend,
  startScheduler,
};
