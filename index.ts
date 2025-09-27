// index.ts
// Fetch txlist, tokentx, txlistinternal from Etherscan (paginated + retries)
// Build summary (buys/sells) and enhance TokenDeployed entries using on-chain event logs via ethers.
//
// Requirements:
//   npm i axios dotenv ethers
//   npm i -D typescript ts-node @types/node
//
// Run (dev): npx ts-node index.ts
// Or compile: npx tsc && node dist/index.js

import axios from "axios";
import * as fs from "fs/promises";
import path from "path";
import dotenv from "dotenv";
import { ethers, Contract } from "ethers";

dotenv.config();

// ---------- provider + helper to query events ----------
function makeProvider(): ethers.Provider {
    if (CONFIG.CHAIN_ID_PARAM === 1) {
        const rpc = process.env.RPC_URL || process.env.ALCHEMY_URL || process.env.INFURA_URL;
        if (rpc) return new ethers.JsonRpcProvider(rpc);
    }
    else if (CONFIG.CHAIN_ID_PARAM === 56) {
        const rpc = process.env.BNB_RPC_URL || process.env.BNB_ALCHEMY_URL || process.env.BNB_INFURA_URL;
        if (rpc) return new ethers.JsonRpcProvider(rpc);
    }
    // fallback to default provider (may be rate-limited)
    return ethers.getDefaultProvider();
}

// narrow a returned log to an ethers EventLog (has .args)
function isEventLog(x: any): x is ethers.EventLog {
    return x && typeof x === "object" && "args" in x && typeof (x as any).args !== "undefined";
}

// ABI fragments for the launcher/market contract: TokenDeployed + Trade
const launcherAbi = [
    "event TokenDeployed(address indexed token, address creator, string myIndex)",
    // Trade event example (adapt if real signature differs):
    "event Trade(address indexed user, address indexed token, bool buy, uint256 inAmt, uint256 outAmt)"
];

// Query TokenDeployed and Trade events over a small block range
async function getEventsByBlockRange(
    contractAddress: string,
    fromBlock: number,
    toBlock?: number | "latest",
    abi: any = launcherAbi
) {
    const provider = makeProvider();
    const contract = new Contract(contractAddress, abi, provider);

    const toBlkArg: number | undefined = typeof toBlock === "undefined" || toBlock === "latest" ? undefined : toBlock;

    const hasTokenDeployedFilter = typeof (contract.filters as any).TokenDeployed === "function";
    const hasTradeFilter = typeof (contract.filters as any).Trade === "function";

    try {
        const logs1 = hasTokenDeployedFilter
            ? await contract.queryFilter((contract.filters as any).TokenDeployed(), fromBlock, toBlkArg)
            : [];

        const logs2 = hasTradeFilter
            ? await contract.queryFilter((contract.filters as any).Trade(), fromBlock, toBlkArg)
            : [];

        return [logs1, logs2];
    } catch (err) {
        console.error("getEventsByBlockRange error:", err);
        return [[], []];
    }
}

// ---------- CONFIG (edit as needed) ----------
const CONFIG = {
    USE_ENV_API_KEY: true,
    API_KEY: process.env.ETHERSCAN_API_KEY,
    // The launcher / contract address to monitor (emits TokenDeployed/Trade)

    ADDRESS: "0x471df254269ebeee55db5e131c3a97e5ec2ba425",

    START_BLOCK: 23339090,
    END_BLOCK: 23392465,

    CHAIN_ID_PARAM: 1 as number | undefined,

    // ADDRESS: "0x5Af2F75116A8a5EaE2EB712ec74a3f90FFa9079b",

    // START_BLOCK: 62512469,
    // END_BLOCK: 62609735,

    // CHAIN_ID_PARAM: 56 as number | undefined,

    OFFSET: 100,
    SORT: "asc" as const,

    FILTER_ERRORS: true,
    FETCH_ERC20_TRANSFERS: true,
    FETCH_INTERNAL_TRANSFERS: true,

    SAVE_JSON_PATH: "",
    SAVE_ERC20_JSON_PATH: "",
    SAVE_INTERNAL_JSON_PATH: "",

    API_BASE: "https://api.etherscan.io/v2/api",

    MAX_RETRIES: 6,
    INITIAL_BACKOFF_MS: 1000,
    PAGE_DELAY_MS: 300,
    MAX_CONSECUTIVE_EMPTY_KEPT_PAGES: 5,
};

// derived filenames
if (!CONFIG.SAVE_JSON_PATH) CONFIG.SAVE_JSON_PATH = `./${CONFIG.ADDRESS.replace(/^0x/i, "").toLowerCase()}-txs.json`;
if (!CONFIG.SAVE_ERC20_JSON_PATH) CONFIG.SAVE_ERC20_JSON_PATH = `./${CONFIG.ADDRESS.replace(/^0x/i, "").toLowerCase()}-tokentx.json`;
if (!CONFIG.SAVE_INTERNAL_JSON_PATH) CONFIG.SAVE_INTERNAL_JSON_PATH = `./${CONFIG.ADDRESS.replace(/^0x/i, "").toLowerCase()}-internal.json`;

// ---------- Types ----------
type EtherscanResponse<T = any> = { status: string; message: string; result: T };
type Tx = any;
type TokenTx = any;
type InternalTx = any;

// ---------- Helpers ----------
function sleep(ms: number) {
    return new Promise((res) => setTimeout(res, ms));
}

async function getWithRetries(url: string, params: Record<string, any>, maxRetries: number, initialBackoffMs: number) {
    let attempt = 0;
    let backoff = initialBackoffMs;
    while (attempt <= maxRetries) {
        attempt++;
        try {
            const resp = await axios.get(url, { params, timeout: 30_000 });
            return resp.data;
        } catch (err: any) {
            const status = err.response?.status;
            const msg = err.message || String(err);
            if (attempt > maxRetries) throw err;
            console.warn(`Request failed: ${msg} (HTTP ${status || "?"}). Retrying in ${backoff}ms...`);
            await sleep(backoff);
            backoff *= 2;
        }
    }
    throw new Error("Retries exhausted");
}

async function fetchTxPage(
    address: string,
    apiKey: string,
    startblock: number,
    endblock: number,
    page: number,
    offset: number,
    sort: string,
    apiBase: string,
    chainIdParam?: number
): Promise<EtherscanResponse<Tx[]>> {
    const params: Record<string, any> = {
        module: "account",
        action: "txlist",
        address,
        startblock,
        endblock,
        page,
        offset,
        sort,
        apikey: apiKey,
    };
    if (typeof chainIdParam !== "undefined") params.chainid = chainIdParam;
    return (await getWithRetries(apiBase, params, CONFIG.MAX_RETRIES, CONFIG.INITIAL_BACKOFF_MS)) as EtherscanResponse<Tx[]>;
}

async function fetchTokenTxPage(
    address: string | undefined,
    apiKey: string,
    startblock: number,
    endblock: number | undefined,
    page: number,
    offset: number,
    sort: string,
    apiBase: string,
    contractAddress?: string,
    chainIdParam?: number
): Promise<EtherscanResponse<TokenTx[]>> {
    const params: Record<string, any> = {
        module: "account",
        action: "tokentx",
        startblock,
        page,
        offset,
        sort,
        apikey: apiKey,
    };
    if (typeof endblock !== "undefined" && endblock !== null) params.endblock = endblock;
    if (contractAddress) params.contractaddress = contractAddress;
    if (address) params.address = address;
    if (typeof chainIdParam !== "undefined") params.chainid = chainIdParam;
    if (!contractAddress && !address) throw new Error("fetchTokenTxPage requires contractAddress or address");
    return (await getWithRetries(apiBase, params, CONFIG.MAX_RETRIES, CONFIG.INITIAL_BACKOFF_MS)) as EtherscanResponse<TokenTx[]>;
}

async function fetchInternalTxPage(
    address: string,
    apiKey: string,
    startblock: number,
    endblock: number,
    page: number,
    offset: number,
    sort: string,
    apiBase: string,
    chainIdParam?: number
): Promise<EtherscanResponse<InternalTx[]>> {
    const params: Record<string, any> = {
        module: "account",
        action: "txlistinternal",
        address,
        startblock,
        endblock,
        page,
        offset,
        sort,
        apikey: apiKey,
    };
    if (typeof chainIdParam !== "undefined") params.chainid = chainIdParam;
    return (await getWithRetries(apiBase, params, CONFIG.MAX_RETRIES, CONFIG.INITIAL_BACKOFF_MS)) as EtherscanResponse<InternalTx[]>;
}

// convert wei string -> decimal string (ETH units)
function weiToEth(weiStr: any) {
    try {
        const wei = BigInt(weiStr ?? "0");
        const base = 10n ** 18n;
        const whole = wei / base;
        const rem = wei % base;
        if (rem === 0n) return whole.toString();
        let frac = rem.toString().padStart(18, "0");
        frac = frac.replace(/0+$/, "");
        return `${whole.toString()}.${frac}`;
    } catch (e) {
        return "";
    }
}
function utcFromTimestamp(ts: any) {
    const n = Number(ts);
    if (!Number.isFinite(n)) return "";
    return new Date(n * 1000).toISOString();
}
function eqAddr(a: any, b: any) {
    if (!a || !b) return false;
    return a.toString().toLowerCase() === b.toString().toLowerCase();
}

// ---------- Main ----------
(async function main() {
    try {
        const apiKey = CONFIG.USE_ENV_API_KEY ? process.env.ETHERSCAN_API_KEY || CONFIG.API_KEY : CONFIG.API_KEY;
        if (!apiKey) console.warn("Warning: no Etherscan API key set; you may hit rate limits.");

        console.log(`Fetching txlist for ${CONFIG.ADDRESS}`);
        console.log(`startblock=${CONFIG.START_BLOCK} endblock=${CONFIG.END_BLOCK} offset=${CONFIG.OFFSET} sort=${CONFIG.SORT}`);

        // 1) txlist
        const allTxs: Tx[] = [];
        let page = 1;
        let consecutiveEmptyKeptPages = 0;
        while (true) {
            console.log(`Requesting txlist page ${page}...`);
            const data = await fetchTxPage(
                CONFIG.ADDRESS,
                apiKey!,
                CONFIG.START_BLOCK,
                CONFIG.END_BLOCK,
                page,
                CONFIG.OFFSET,
                CONFIG.SORT,
                CONFIG.API_BASE,
                CONFIG.CHAIN_ID_PARAM
            );

            if (data.status === "0") {
                if (Array.isArray(data.result) && data.result.length === 0) console.log("No more transactions returned by API (txlist).");
                else console.warn(`Etherscan returned status=0 for txlist: ${data.message}`);
                break;
            }
            if (!Array.isArray(data.result)) {
                console.warn("Unexpected txlist response format:", data);
                break;
            }

            const rawTxs = data.result;
            const kept = CONFIG.FILTER_ERRORS ? rawTxs.filter((tx: any) => tx.isError !== "1") : rawTxs;
            console.log(`Got ${rawTxs.length} txs on page ${page} (${kept.length} kept).`);
            allTxs.push(...kept);

            if (kept.length === 0) consecutiveEmptyKeptPages++;
            else consecutiveEmptyKeptPages = 0;
            if (consecutiveEmptyKeptPages >= CONFIG.MAX_CONSECUTIVE_EMPTY_KEPT_PAGES) break;
            if (rawTxs.length < CONFIG.OFFSET) break;
            page++;
            await sleep(CONFIG.PAGE_DELAY_MS);
        }
        await fs.writeFile(path.resolve(CONFIG.SAVE_JSON_PATH), JSON.stringify(allTxs, null, 2), "utf8");
        console.log(`Saved ${allTxs.length} transactions to ${CONFIG.SAVE_JSON_PATH}`);

        // 2) tokentx
        let allTokenTxs: TokenTx[] = [];
        if (CONFIG.FETCH_ERC20_TRANSFERS) {
            console.log("Fetching tokentx...");
            let tpage = 1;
            while (true) {
                console.log(`Requesting tokentx page ${tpage}...`);
                const tdata = await fetchTokenTxPage(
                    CONFIG.ADDRESS,
                    apiKey!,
                    CONFIG.START_BLOCK,
                    CONFIG.END_BLOCK,
                    tpage,
                    CONFIG.OFFSET,
                    CONFIG.SORT,
                    CONFIG.API_BASE,
                    undefined,
                    CONFIG.CHAIN_ID_PARAM
                );

                if (tdata.status === "0") {
                    if (Array.isArray(tdata.result) && tdata.result.length === 0) console.log("No more token transfers returned by API.");
                    else console.warn(`Etherscan returned status=0 for tokentx: ${tdata.message}`);
                    break;
                }
                if (!Array.isArray(tdata.result)) {
                    console.warn("Unexpected tokentx response format:", tdata);
                    break;
                }
                const raw = tdata.result;
                console.log(`Got ${raw.length} token transfers on page ${tpage}.`);
                allTokenTxs.push(...raw);

                if (raw.length < CONFIG.OFFSET) break;
                tpage++;
                await sleep(CONFIG.PAGE_DELAY_MS);
            }
            await fs.writeFile(path.resolve(CONFIG.SAVE_ERC20_JSON_PATH), JSON.stringify(allTokenTxs, null, 2), "utf8");
            console.log(`Saved ${allTokenTxs.length} token transfers to ${CONFIG.SAVE_ERC20_JSON_PATH}`);
        }

        // 3) internal txs
        let allInternal: InternalTx[] = [];
        if (CONFIG.FETCH_INTERNAL_TRANSFERS) {
            console.log("Fetching internal txs...");
            let ipage = 1;
            while (true) {
                console.log(`Requesting internal tx page ${ipage}...`);
                const idata = await fetchInternalTxPage(
                    CONFIG.ADDRESS,
                    apiKey!,
                    CONFIG.START_BLOCK,
                    CONFIG.END_BLOCK,
                    ipage,
                    CONFIG.OFFSET,
                    CONFIG.SORT,
                    CONFIG.API_BASE,
                    CONFIG.CHAIN_ID_PARAM
                );

                if (idata.status === "0") {
                    if (Array.isArray(idata.result) && idata.result.length === 0) console.log("No more internal transactions returned by API.");
                    else console.warn(`Etherscan returned status=0 for txlistinternal: ${idata.message}`);
                    break;
                }
                if (!Array.isArray(idata.result)) {
                    console.warn("Unexpected txlistinternal response format:", idata);
                    break;
                }
                const raw = idata.result;
                console.log(`Got ${raw.length} internal txs on page ${ipage}.`);
                allInternal.push(...raw);

                if (raw.length < CONFIG.OFFSET) break;
                ipage++;
                await sleep(CONFIG.PAGE_DELAY_MS);
            }
            await fs.writeFile(path.resolve(CONFIG.SAVE_INTERNAL_JSON_PATH), JSON.stringify(allInternal, null, 2), "utf8");
            console.log(`Saved ${allInternal.length} internal transactions to ${CONFIG.SAVE_INTERNAL_JSON_PATH}`);
        }

        // -----------------------
        // Build summary
        // -----------------------
        try {
            const tokentxRaw = await fs.readFile(path.resolve(CONFIG.SAVE_ERC20_JSON_PATH), "utf8");
            const txsRaw = await fs.readFile(path.resolve(CONFIG.SAVE_JSON_PATH), "utf8");
            const internalsRaw = await fs.readFile(path.resolve(CONFIG.SAVE_INTERNAL_JSON_PATH), "utf8");

            const tokentxData = JSON.parse(tokentxRaw);
            const txsData = JSON.parse(txsRaw);
            const internalsData = JSON.parse(internalsRaw);

            const tokenArray: any[] = Array.isArray(tokentxData) ? tokentxData : tokentxData?.result ?? [];
            const txsArray: any[] = Array.isArray(txsData) ? txsData : txsData?.result ?? [];
            const internalsArray: any[] = Array.isArray(internalsData) ? internalsData : internalsData?.result ?? [];

            // index txs and internals by hash
            const txsByHash = new Map<string, any[]>();
            for (const t of txsArray) {
                if (t && t.hash) {
                    const k = t.hash.toLowerCase();
                    if (!txsByHash.has(k)) txsByHash.set(k, []);
                    txsByHash.get(k)!.push(t);
                }
            }
            const internalsByHash = new Map<string, any[]>();
            for (const it of internalsArray) {
                if (it && it.hash) {
                    const k = it.hash.toLowerCase();
                    if (!internalsByHash.has(k)) internalsByHash.set(k, []);
                    internalsByHash.get(k)!.push(it);
                }
            }

            const summary: any[] = [];
            const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";
            const seenTxnHashes = new Set<string>();

            // Group token transfers by txn hash
            const tokenGroups = new Map<string, any[]>();
            for (const tk of tokenArray) {
                if (!tk) continue;
                const h = (tk.hash || "").toString().toLowerCase();
                const key = h || `nohash-${Math.random().toString(36).slice(2, 9)}`;
                if (!tokenGroups.has(key)) tokenGroups.set(key, []);
                tokenGroups.get(key)!.push(tk);
            }

            // Build initial summary entries (buys/sells + TokenDeployed detections)
            for (const [txnHash, group] of tokenGroups.entries()) {
                if (txnHash && seenTxnHashes.has(txnHash)) continue;

                const hasZeroFrom = group.some((g) => ((g.from || "").toString().toLowerCase() === ZERO_ADDRESS));
                const hasNonZeroFrom = group.some((g) => {
                    const f = (g.from || "").toString().toLowerCase();
                    return f && f !== ZERO_ADDRESS;
                });

                // Handle TokenDeployed detection for all zero-address to CONFIG.ADDRESS transfers
                let handled = false;
                if (hasZeroFrom) {
                    const zeroEntries = group.filter(g => ((g.from || "").toString().toLowerCase() === ZERO_ADDRESS));

                    for (const zeroEntry of zeroEntries) {
                        if (eqAddr(zeroEntry.to, CONFIG.ADDRESS)) {
                            // token deployed to monitored address -> record TokenDeployed summary
                            summary.push({
                                type: "TokenDeployed",
                                tokenAddress: zeroEntry.contractAddress ?? zeroEntry.contractaddress ?? "",
                                tokenCreator: zeroEntry.to ?? "",
                                transactionHash: zeroEntry.hash ?? "",
                                timestamp: zeroEntry.timeStamp ?? "",
                                blockNumber: zeroEntry.blockNumber ?? "",
                                eventDecoded: [],
                                relatedTrades: [],
                            });
                            if (zeroEntry.hash) seenTxnHashes.add(zeroEntry.hash.toLowerCase());
                            handled = true;
                        }
                    }

                    // Skip buy/sell processing if this was a pure TokenDeployed transaction
                    if (handled && !hasNonZeroFrom) {
                        continue;
                    }
                }

                // If group contains both zero-from and non-zero-from entries and we found TokenDeployed:
                // skip the rest of this group regardless (already handled TokenDeployed above)
                if (hasZeroFrom && hasNonZeroFrom && handled) {
                    continue;
                }

                // Otherwise choose representative (prefer non-zero-from)
                const rep = group.find((g) => (g.from || "").toLowerCase() !== ZERO_ADDRESS) ?? group[0];
                if (!rep) continue;

                const repHashRaw = (rep.hash || "").toString();
                const repHash = repHashRaw ? repHashRaw.toLowerCase() : "";
                const repFrom = (rep.from || "").toString();
                const repTo = (rep.to || "").toString();
                const isRepFromZero = repFrom.toLowerCase() === ZERO_ADDRESS;
                if (!isRepFromZero && repHash && seenTxnHashes.has(repHash)) continue;

                const tokenValueRaw = weiToEth(rep.value) ?? weiToEth(rep.tokenValue) ?? weiToEth(rep.amount) ?? "";
                const dateUtc = utcFromTimestamp(rep.timeStamp ?? rep.timeStamp ?? rep.timeStamp);
                const fname = ((rep.functionName || "") as string).toLowerCase();

                if (fname) {
                    const isBuy = fname.includes("buy(") || fname.startsWith("buy ");
                    const isSell = fname.includes("sell(") || fname.startsWith("sell ");
                    if (isBuy || isSell) {
                        handled = true;
                        const type = isBuy ? "buy" : "sell";
                        const wallet = isBuy ? repTo : repFrom;

                        let ethAmount = "";
                        if (type === "sell") {
                            const candidates = internalsByHash.get(repHash) || [];
                            let sum = 0n;
                            for (const it of candidates) {
                                if (it.to && wallet && eqAddr(it.to, wallet)) {
                                    try {
                                        sum += BigInt(it.value ?? it.amount ?? "0");
                                    } catch (e) { }
                                }
                            }
                            if (sum > 0n) ethAmount = weiToEth(sum.toString());
                        } else {
                            const candidates = txsByHash.get(repHash) || [];
                            const match = candidates.find((t) => t.from && wallet && eqAddr(t.from, wallet));
                            if (match) ethAmount = weiToEth(match.value ?? "0");
                            else if (candidates.length > 0) ethAmount = weiToEth(candidates[0].value ?? "0");
                        }

                        summary.push({
                            type,
                            marketCap: "",
                            wallet: wallet || "",
                            tokenAddress: rep.contractAddress ?? rep.contractaddress ?? "",
                            ethAmount,
                            tokenAmount: tokenValueRaw?.toString?.() ?? "",
                            TxnHash: isRepFromZero ? "" : rep.hash ?? "",
                            "Date/TIme (UTC)": dateUtc || "",
                        });

                        if (!isRepFromZero && repHash) seenTxnHashes.add(repHash);
                    }
                }
                if (handled) continue;

                // fallback: internal-matching rules (no functionName)
                if (repTo) {
                    const internalCandidates = internalsByHash.get(repHash) || [];
                    const buyMatches = internalCandidates.filter((it) => it.from && eqAddr(it.from, repTo));
                    if (buyMatches.length > 0) {
                        let sum = 0n;
                        for (const it of buyMatches) {
                            try {
                                sum += BigInt(it.value ?? it.amount ?? "0");
                            } catch (e) { }
                        }
                        const ethAmount = sum > 0n ? weiToEth(sum.toString()) : "";
                        summary.push({
                            type: "buy",
                            marketCap: "",
                            wallet: repTo,
                            tokenAddress: rep.contractAddress ?? rep.contractaddress ?? "",
                            ethAmount,
                            tokenAmount: tokenValueRaw?.toString?.() ?? "",
                            TxnHash: isRepFromZero ? "" : rep.hash ?? "",
                            "Date/TIme (UTC)": dateUtc || "",
                        });
                        if (!isRepFromZero && repHash) seenTxnHashes.add(repHash);
                        continue;
                    }
                }

                if (repFrom) {
                    const internalCandidates = internalsByHash.get(repHash) || [];
                    const sellMatches = internalCandidates.filter((it) => it.to && eqAddr(it.to, repFrom));
                    if (sellMatches.length > 0) {
                        let sum = 0n;
                        for (const it of sellMatches) {
                            try {
                                sum += BigInt(it.value ?? it.amount ?? "0");
                            } catch (e) { }
                        }
                        const ethAmount = sum > 0n ? weiToEth(sum.toString()) : "";
                        summary.push({
                            type: "sell",
                            marketCap: "",
                            wallet: repFrom,
                            tokenAddress: rep.contractAddress ?? rep.contractaddress ?? "",
                            ethAmount,
                            tokenAmount: tokenValueRaw?.toString?.() ?? "",
                            TxnHash: isRepFromZero ? "" : rep.hash ?? "",
                            "Date/TIme (UTC)": dateUtc || "",
                        });
                        if (!isRepFromZero && repHash) seenTxnHashes.add(repHash);
                        continue;
                    }
                }

                // else skip
            }

            // -----------------------
            // ENRICH TokenDeployed entries using on-chain events
            // -----------------------
            const provider = makeProvider();

            for (let i = 0; i < summary.length; i++) {
                const entry = summary[i];
                if (!entry || entry.type !== "TokenDeployed") continue;

                const blockNumberRaw = entry.blockNumber;
                const blockNumber = Number(blockNumberRaw);
                if (!Number.isFinite(blockNumber)) continue;

                const fromBlock = Math.max(0, blockNumber - 1);
                const toBlock = blockNumber + 1;

                console.log(`Querying events around block ${blockNumber} (${fromBlock}-${toBlock}) for launcher ${CONFIG.ADDRESS}`);

                try {
                    const [tokenDeployedEvents, tradeEvents] = await getEventsByBlockRange(CONFIG.ADDRESS, fromBlock, toBlock, launcherAbi);

                    // attach TokenDeployed event(s) (use guard)
                    entry.eventDecoded = (tokenDeployedEvents || []).map((ev) => {
                        if (!isEventLog(ev)) {
                            return { raw: ev, parsed: false };
                        }

                        const args = ev.args; // safe now
                        const tokenAddr = args[0] ? args[0].toString() : null;
                        const creator = args[1] ? args[1].toString() : null;
                        const indexStr = args[2] ? args[2].toString() : null;

                        return {
                            txHash: ev.transactionHash,
                            blockNumber: ev.blockNumber,
                            logIndex: (ev as any).logIndex,
                            tokenAddress: tokenAddr,
                            tokenCreator: creator,
                            index: indexStr,
                            rawArgs: args,
                        };
                    });

                    entry.relatedTrades = entry.relatedTrades || [];

                    for (const tevRaw of (tradeEvents || [])) {
                        if (!isEventLog(tevRaw)) {
                            // skip or keep raw
                            continue;
                        }
                        const tev = tevRaw; // typed as EventLog
                        const a = tev.args ?? [];
                        const user = a[0] ? a[0].toString() : "";
                        const tokenAddr = a[1] ? a[1].toString() : "";
                        const buyFlag = !!a[2];
                        const inAmt = a[3] !== undefined ? a[3].toString() : "0";
                        const outAmt = a[4] !== undefined ? a[4].toString() : "0";

                        // derive timestamp from block if available
                        let tsUtc = "";
                        if (typeof tev.blockNumber === "number") {
                            try {
                                const blk = await provider.getBlock(tev.blockNumber);
                                if (blk?.timestamp) tsUtc = new Date(blk.timestamp * 1000).toISOString();
                            } catch (err) { /* ignore */ }
                        }

                        const ethAmount = weiToEth(inAmt);
                        const tokenAmount = weiToEth(outAmt);

                        const tradeEntry = {
                            type: buyFlag ? "buy" : "sell",
                            marketCap: "",
                            wallet: user || "",
                            tokenAddress: tokenAddr || "",
                            ethAmount: ethAmount || "",
                            tokenAmount: tokenAmount || "",
                            TxnHash: tev.transactionHash ?? "",
                            "Date/TIme (UTC)": tsUtc || "",
                            derivedFromEvent: true,
                        };

                        const txLower = (tev.transactionHash || "").toLowerCase();
                        if (!txLower || !seenTxnHashes.has(txLower)) {
                            summary.push(tradeEntry);
                            if (txLower) seenTxnHashes.add(txLower);
                        }

                        entry.relatedTrades.push({
                            transactionHash: tev.transactionHash,
                            blockNumber: tev.blockNumber,
                            event: { user, token: tokenAddr, buy: buyFlag, inAmt, outAmt },
                        });
                    }
                } catch (err) {
                    console.warn("Failed to query events for TokenDeployed entry:", err);
                }
            }

            // final write of summary
            const summaryPath = path.resolve(`./${CONFIG.ADDRESS.replace(/^0x/i, "").toLowerCase()}-summary.json`);
            await fs.writeFile(summaryPath, JSON.stringify(summary, null, 2), "utf8");
            console.log(`Saved ${summary.length} summary entries to ${summaryPath}`);

        } catch (e) {
            console.error("Failed to build summary:", e);
        }
    } catch (err) {
        console.error("Fatal error:", err);
        process.exit(1);
    }
})();