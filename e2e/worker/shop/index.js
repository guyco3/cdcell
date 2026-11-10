// Demo Durable Object for cdcell's end-to-end tests.
//
// Each Shop is one cell with its own SQLite database. Its mutations cover the
// cases the LTX -> row decoder must handle:
//   orders       rowid table (INTEGER PRIMARY KEY), inserts/updates/deletes
//   items        WITHOUT ROWID table with a composite primary key
//   attachments  blobs of 5-20 KB, so rows spill into overflow pages, and
//                same-size overwrites that change only overflow bytes
//   ctx.storage  the KV API (_cf_KV), values are V8-serialized
//
//   GET /?shop=NAME&op=random   one random mutation (default op)
//   GET /?shop=NAME&op=dump     the full current state, for verification
export class Shop {
  constructor(ctx) {
    this.ctx = ctx;
    this.sql = ctx.storage.sql;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS orders (
      id INTEGER PRIMARY KEY, customer TEXT NOT NULL, status TEXT NOT NULL,
      total REAL NOT NULL, updated_at INTEGER NOT NULL)`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS items (
      order_id INTEGER NOT NULL, sku TEXT NOT NULL, qty INTEGER NOT NULL,
      PRIMARY KEY (order_id, sku)) WITHOUT ROWID`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS attachments (
      id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL, data BLOB NOT NULL)`);
  }

  async fetch(request) {
    const op = new URL(request.url).searchParams.get("op") ?? "random";
    if (op === "dump") return Response.json(await this.dump());
    const done = await this.mutate(op === "random" ? pick(OPS) : op);
    return Response.json({ op: done });
  }

  async mutate(op) {
    const now = Date.now();
    const anyOrder = () =>
      this.sql.exec("SELECT id FROM orders ORDER BY random() LIMIT 1").toArray()[0]?.id;
    switch (op) {
      case "create": {
        // One statement group in one transaction: an order plus its items.
        this.ctx.storage.transactionSync(() => {
          const id = this.sql.exec(
            "INSERT INTO orders (customer, status, total, updated_at) VALUES (?, 'new', ?, ?) RETURNING id",
            pick(CUSTOMERS), round(Math.random() * 500), now).one().id;
          for (const sku of sample(SKUS, 1 + rnd(3))) {
            this.sql.exec("INSERT INTO items VALUES (?, ?, ?)", id, sku, 1 + rnd(5));
          }
        });
        return op;
      }
      case "update": {
        const id = anyOrder();
        if (id === undefined) return this.mutate("create");
        this.sql.exec("UPDATE orders SET status = ?, updated_at = ? WHERE id = ?",
          pick(STATUSES), now, id);
        return op;
      }
      case "bulk": {
        // One statement touching many rows.
        this.sql.exec("UPDATE orders SET total = round(total * 1.1, 2), updated_at = ? WHERE status = 'new'", now);
        return op;
      }
      case "delete": {
        const id = anyOrder();
        if (id === undefined) return this.mutate("create");
        this.ctx.storage.transactionSync(() => {
          this.sql.exec("DELETE FROM items WHERE order_id = ?", id);
          this.sql.exec("DELETE FROM attachments WHERE order_id = ?", id);
          this.sql.exec("DELETE FROM orders WHERE id = ?", id);
        });
        return op;
      }
      case "attach": {
        const id = anyOrder();
        if (id === undefined) return this.mutate("create");
        this.sql.exec("INSERT INTO attachments (order_id, data) VALUES (?, ?)", id, blob(5000 + rnd(15000)));
        return op;
      }
      case "retouch": {
        // Same-size overwrite that changes only the last byte: SQLite can
        // rewrite just the overflow page and leave the leaf page untouched.
        const row = this.sql.exec("SELECT id, data FROM attachments ORDER BY random() LIMIT 1").toArray()[0];
        if (!row) return this.mutate("attach");
        const data = new Uint8Array(row.data);
        data[data.length - 1] ^= 0xff;
        this.sql.exec("UPDATE attachments SET data = ? WHERE id = ?", data, row.id);
        return op;
      }
      case "kv": {
        await this.ctx.storage.put(`pref:${pick(CUSTOMERS)}`, { theme: pick(["dark", "light"]), at: now });
        return op;
      }
      default:
        throw new Error(`unknown op ${op}`);
    }
  }

  async dump() {
    const rows = (q) => this.sql.exec(q).toArray();
    return {
      orders: rows("SELECT * FROM orders ORDER BY id"),
      items: rows("SELECT * FROM items ORDER BY order_id, sku"),
      attachments: rows("SELECT id, order_id, length(data) AS bytes, hex(substr(data, -4)) AS tail FROM attachments ORDER BY id"),
      kv: Object.fromEntries(await this.ctx.storage.list({ prefix: "pref:" })),
    };
  }
}

export default {
  async fetch(request, env) {
    const shop = new URL(request.url).searchParams.get("shop") ?? "default";
    return env.SHOP.get(env.SHOP.idFromName(shop)).fetch(request);
  },
};

// Weighted toward the common case; deletes and bulk updates are rarer.
const OPS = ["create", "create", "create", "update", "update", "update", "update",
             "bulk", "delete", "attach", "retouch", "retouch", "kv"];
const CUSTOMERS = ["ada", "bo", "cy", "dee", "eli", "fay", "gus"];
const STATUSES = ["paid", "packed", "shipped", "delivered", "refunded"];
const SKUS = ["apple", "bread", "cheese", "dates", "eggs", "figs"];
const rnd = (n) => Math.floor(Math.random() * n);
const pick = (xs) => xs[rnd(xs.length)];
const sample = (xs, n) => [...xs].sort(() => Math.random() - 0.5).slice(0, n);
const round = (x) => Math.round(x * 100) / 100;
const blob = (n) => { const b = new Uint8Array(n); crypto.getRandomValues(b); return b; };
