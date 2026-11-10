import http from 'k6/http';
import crypto from 'k6/crypto';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const SECRET = 'abeli';
const URL = 'http://localhost:8080/webhook';

export const options = {
  scenarios: {
    stress_test: {
      executor: 'ramping-vus',
      startVUs: 10,
      stages: [
        { duration: '10s', target: 50 }, 
        { duration: '30s', target: 100 }, 
        { duration: '20s', target: 150 }, 
        { duration: '10s', target: 0 }, 
      ],
      gracefulRampDown: '5s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'], 
    http_req_duration: ['p(95)<1000'],
  },
};

// Maintain a sliding window of recent event IDs to simulate realistic ~10% webhook retries
const recentEventIds = [];

export default function () {
  const event = generateEvent();
  const body = JSON.stringify(event);

  // Stripe/Paystack-style HMAC-SHA256 over "{timestamp}.{body}"
  const timestamp = Math.floor(Date.now() / 1000);
  const signature = crypto.hmac(
    'sha256',
    SECRET,
    `${timestamp}.${body}`,
    'hex'
  );

  const headers = {
    'Content-Type': 'application/json',
    'X-PSP-Signature': `t=${timestamp},v1=${signature}`,
  };

  http.post(URL, body, { headers });
}

function generateEvent() {
  const accountIdx = Math.floor(Math.random() * 50);
  // Realistic 10-digit virtual account number with "6930" prefix (e.g. 6930000001 - 6930000050)
  const accountNumber = `6930${String(accountIdx + 1).padStart(6, '0')}`;

  // 10% chance to simulate duplicate webhook delivery
  const isRetry = recentEventIds.length > 50 && Math.random() < 0.10;
  const eventId = isRetry
    ? recentEventIds[Math.floor(Math.random() * recentEventIds.length)]
    : `evt_${uuidv4()}`;

  if (!isRetry) {
    recentEventIds.push(eventId);
    if (recentEventIds.length > 500) recentEventIds.shift();
  }

  return {
    event_id: eventId,
    event_type: 'payment.succeeded',
    occurred_at: new Date().toISOString(),
    provider: 'fake-psp',
    payment_id: `pay_${uuidv4()}`,
    account_id: accountNumber,
    sequence: 1,
    amount: Math.floor(Math.random() * 100000) + 100,
    currency: 'NGN',
    data: {
      method: 'bank_transfer',
      bank: 'Bankee',
      customer_id: `cus_${uuidv4().substring(0, 8)}`,
    },
  };
}
