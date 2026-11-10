// Amounts arrive in minor units (kopecks).

const roubles = new Intl.NumberFormat("en-GB", { style: "currency", currency: "RUB", currencyDisplay: "narrowSymbol", maximumFractionDigits: 0 });
const compactRoubles = new Intl.NumberFormat("en-GB", { style: "currency", currency: "RUB", currencyDisplay: "narrowSymbol", notation: "compact", maximumFractionDigits: 1 });
const count = new Intl.NumberFormat("en-GB");
const share = new Intl.NumberFormat("en-GB", { style: "percent", maximumFractionDigits: 1 });

export const rub = (minor: number) => roubles.format(minor / 100);
export const rubShort = (minor: number) => compactRoubles.format(minor / 100);
export const n = (x: number) => count.format(x);
export const pct = (x: number) => share.format(x);
export const secs = (s: number) => (s < 10 ? s.toFixed(1) : Math.round(s).toString()) + " s";
export const ms = (x: number) => (x < 1000 ? `${Math.round(x)} ms` : `${(x / 1000).toFixed(1)} s`);

/** A ClickHouse time ("2026-11-10 03:12:00", UTC) as a local clock time. */
export function clock(t: string, withSeconds = false): string {
  const d = new Date(t.replace(" ", "T") + "Z");
  return d.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", ...(withSeconds ? { second: "2-digit" } : {}) });
}

/** What each model feature means, for the reasons a payment was declined. */
export const REASONS: Record<string, string> = {
  n_1m: "Many attempts in a minute",
  n_10m: "Many attempts in 10 minutes",
  n_1h: "Many attempts in an hour",
  sum_10m_log: "Much spent in 10 minutes",
  sum_1h_log: "Much spent in an hour",
  merchants_10m: "Many merchants in 10 minutes",
  declines_10m: "Recent declines",
  small_10m: "Tiny test payments",
  history_log: "Little history",
  amount_log: "A large amount",
  amount_z: "Unusual for this customer",
  amount_vs_max: "Above the customer's largest",
  new_merchant: "A merchant never used before",
  merchant_share: "A merchant rarely used",
  since_last_log: "Soon after the last attempt",
  merchant_n_10m: "A busy merchant",
};

/** "sim-1a1c36174f5:m10" -> "m10": the run prefix is noise on screen. */
export const shortCode = (code: string) => code?.split(":").pop() ?? code;
