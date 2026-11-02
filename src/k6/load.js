import http from 'k6/http';
import crypto from 'k6/crypto';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

const SECRET = 'abeli';
const URL = 'http://localhost:8080/webhook';

export const options = {
  iterations: 50_000,
};

export default function () {
  const event = generateEvent();
  const body = JSON.stringify(event);

  // Stripe-style: HMAC-SHA256 over "{timestamp}.{body}"
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
  return {
    event_id: `evt_${uuidv4()}`,
    event_type: 'payment.succeeded',
    occurred_at: new Date().toISOString(),
    provider: 'fake-psp',
    payment_id: `pay_${uuidv4()}`,
    account_id: `acct_${uuidv4()}`,
    sequence: 1,
    amount: Math.floor(Math.random() * 100000),
    currency: 'NGN',
    data: {
      method: 'card',
      last4: '4242',
      customer_id: `cus_${uuidv4()}`,
    },
  };
}