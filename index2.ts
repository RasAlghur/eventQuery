// listen.robust.ts
import fs from "fs/promises";
import path from "path";
import { ethers, Contract } from "ethers";
import dotenv from "dotenv";
dotenv.config();

const WSS = process.env.WSS_URL ?? "";
// const CONTRACT = process.env.CONTRACT_ADDRESS ?? "0x77737c9b388C53c1d9955408E8418472805d7e16";
const CONTRACT = process.env.CONTRACT_ADDRESS ?? "0x15be8db6da1b66ceae88b1698d4fb149f8559bd1";
const ABI = [
    "event TokenDeployed(address indexed token, address creator, string myIndex)",
    "event Trade(address indexed user, address indexed token, bool buy, uint256 inAmt, uint256 outAmt)",
    "event Transfer(address indexed from, address indexed to, uint amount)"
];

if (!WSS || !CONTRACT) {
    console.error("Set WSS_URL and CONTRACT_ADDRESS in .env");
    process.exit(1);
}

const STATE_DIR = path.resolve(process.cwd(), ".listener_state");
const CHECKPOINT_FILE = path.join(STATE_DIR, "checkpoint.json");
const PROCESSED_FILE = path.join(STATE_DIR, "processed.jsonl");

async function ensureStateDir(): Promise<void> {
    try {
        await fs.mkdir(STATE_DIR, { recursive: true });
    } catch {
        /* ignore */
    }
}

async function readCheckpoint(): Promise<{ lastBlock: number }> {
    try {
        const raw = await fs.readFile(CHECKPOINT_FILE, "utf8");
        return JSON.parse(raw) as { lastBlock: number };
    } catch {
        return { lastBlock: 0 };
    }
}

async function writeCheckpoint(obj: { lastBlock: number }) {
    await fs.writeFile(CHECKPOINT_FILE, JSON.stringify(obj, null, 2));
}

async function markProcessed(txHash: string, logIndex: number) {
    const line = `${txHash}:${logIndex}\n`;
    await fs.appendFile(PROCESSED_FILE, line);
}

async function alreadyProcessed(txHash: string, logIndex: number): Promise<boolean> {
    try {
        const raw = await fs.readFile(PROCESSED_FILE, "utf8");
        return raw.includes(`${txHash}:${logIndex}`);
    } catch {
        return false;
    }
}

function parseEventArgs(args: unknown[]): { payload: unknown[]; event: any } {
    const event = args[args.length - 1] as any;
    const payload = args.slice(0, Math.max(0, args.length - 1));
    return { payload, event };
}

async function backfillLogs(
    provider: ethers.WebSocketProvider,
    contract: Contract,
    fromBlock: number,
    toBlock: number,
    chunk = 2000,
) {
    if (fromBlock > toBlock) return;
    console.log(`Backfilling logs from ${fromBlock} to ${toBlock}`);

    for (let start = fromBlock; start <= toBlock; start += chunk) {
        const end = Math.min(start + chunk - 1, toBlock);
        try {
            const logs = await provider.getLogs({
                address: CONTRACT,
                fromBlock: start,
                toBlock: end,
            });

            for (const log of logs) {
                try {
                    const parsed = contract.interface.parseLog(log as any);
                    const txHash = String((log as any).transactionHash);
                    const logIndex = Number((log as any).logIndex ?? (log as any).index ?? 0);

                    if (await alreadyProcessed(txHash, logIndex)) continue;

                    if (parsed?.name === "TokenDeployed") {
                        console.log("[backfill] TokenDeployed", parsed.args);
                    } else if (parsed?.name === "Trade") {
                        console.log("[backfill] Trade", parsed.args);
                    } else if (parsed?.name === "Transfer") {
                        console.log("[backfill] Transfer", parsed.args);
                    }

                    await markProcessed(txHash, logIndex);
                    await writeCheckpoint({ lastBlock: Number((log as any).blockNumber) });
                } catch (err) {
                    console.warn("failed parsing log:", err);
                }
            }
        } catch (err) {
            console.error("getLogs failed", start, end, err);
            // Reduce chunk size on failure
            if (chunk > 500) {
                console.log(`Reducing chunk size from ${chunk} to ${Math.floor(chunk / 2)}`);
                await backfillLogs(provider, contract, start, toBlock, Math.floor(chunk / 2));
                return;
            }
        }
    }
}

async function run() {
    await ensureStateDir();

    let attempt = 0;
    while (true) {
        let provider: ethers.WebSocketProvider | null = null;
        let contract: Contract | null = null;

        try {
            console.log(`Connecting to WebSocket... (attempt ${attempt + 1})`);
            provider = new ethers.WebSocketProvider(WSS);
            contract = new Contract(CONTRACT, ABI, provider);

            // Set up provider event handlers using ethers.js public API
            provider.on('error', (error) => {
                console.error("Provider error:", error);
            });

            // Test connection
            const blockNumber = await provider.getBlockNumber();
            console.log(`WebSocket connected successfully. Current block: ${blockNumber}`);

            // Reset attempt counter on successful connection
            attempt = 0;

            // Backfill from checkpoint -> latest BEFORE subscribing
            const { lastBlock } = await readCheckpoint();
            const latest = await provider.getBlockNumber();

            console.log(`Last processed block: ${lastBlock}, Current block: ${latest}`);

            if (lastBlock + 1 <= latest) {
                const from = Math.max(lastBlock + 1, latest - 5000);
                await backfillLogs(provider, contract, from, latest);
            }

            // Subscribe to live events
            const tokenDeployedHandler = async (...args: unknown[]) => {
                const { payload, event } = parseEventArgs(args);
                const txHash = event.transactionHash ?? event.log?.transactionHash;
                const logIndex = Number(event.logIndex ?? event.log?.logIndex ?? 0);

                if (typeof txHash !== "string") {
                    console.warn("TokenDeployed event missing transactionHash");
                    return;
                }

                if (await alreadyProcessed(txHash, logIndex)) return;

                console.log("TokenDeployed LIVE:", payload);

                await markProcessed(txHash, logIndex);
                await writeCheckpoint({
                    lastBlock: Number(event.blockNumber ?? event.log?.blockNumber ?? 0)
                });
            };

            const tradeHandler = async (...args: unknown[]) => {
                const { payload, event } = parseEventArgs(args);
                const txHash = event.transactionHash ?? event.log?.transactionHash;
                const logIndex = Number(event.logIndex ?? event.log?.logIndex ?? 0);

                if (typeof txHash !== "string") {
                    console.warn("Trade event missing transactionHash");
                    return;
                }

                if (await alreadyProcessed(txHash, logIndex)) return;

                const payloadArr = payload as any[];
                console.log(
                    `Trade LIVE for for ${txHash} /n Txn Performed:
                    ${payloadArr.map((p) => (p?.toString ? p.toString() : p))}`
                );

                await markProcessed(txHash, logIndex);
                await writeCheckpoint({
                    lastBlock: Number(event.blockNumber ?? event.log?.blockNumber ?? 0)
                });
            };

            const transferHandler = async (...args: unknown[]) => {
                const { payload, event } = parseEventArgs(args);
                const txHash = event.transactionHash ?? event.log?.transactionHash;
                const logIndex = Number(event.logIndex ?? event.log?.logIndex ?? 0);

                if (typeof txHash !== "string") {
                    console.warn("Transfer event missing transactionHash");
                    return;
                }

                if (await alreadyProcessed(txHash, logIndex)) return;

                const payloadArr = payload as any[];

                console.log(
                    `Transfer LIVE for for ${txHash} /n Txn Performed:
                    ${payloadArr.map((p) => (p?.toString ? p.toString() : p))}`
                );

                await markProcessed(txHash, logIndex);
                await writeCheckpoint({
                    lastBlock: Number(event.blockNumber ?? event.log?.blockNumber ?? 0)
                });
            };

            contract.on("TokenDeployed", tokenDeployedHandler);
            contract.on("Trade", tradeHandler);
            contract.on("Transfer", transferHandler);

            console.log("Subscribed to contract events. Waiting for live events...");

            // Wait for disconnect using a combination of health checks and promise that never resolves
            // We'll rely on errors to break out of this
            await new Promise<void>(async (resolve, reject) => {
                let healthCheckInterval: NodeJS.Timeout;
                let isHealthy = true;

                const cleanup = () => {
                    clearInterval(healthCheckInterval);
                };

                // Health check to detect stale connections
                healthCheckInterval = setInterval(async () => {
                    try {
                        await provider!.getBlockNumber();
                        // Connection is still healthy
                    } catch (error) {
                        console.error("Health check failed:", error);
                        isHealthy = false;
                        cleanup();
                        reject(new Error("Health check failed"));
                    }
                }, 30000);

                // Also set a longer timeout to detect completely dead connections
                const deadConnectionTimeout = setTimeout(() => {
                    if (!isHealthy) {
                        cleanup();
                        reject(new Error("Connection appears to be dead"));
                    }
                }, 120000);

                // Cleanup on exit
                const cleanupOnExit = () => {
                    cleanup();
                    clearTimeout(deadConnectionTimeout);
                };

                process.on('SIGINT', cleanupOnExit);
                process.on('SIGTERM', cleanupOnExit);
            });

        } catch (err) {
            console.error("Listener error:", err);

            // Clean up
            if (contract) {
                try {
                    contract.removeAllListeners();
                } catch (e) {
                    // Ignore cleanup errors
                }
            }

            if (provider) {
                try {
                    await provider.destroy();
                } catch (cleanupError) {
                    // Ignore cleanup errors
                }
            }

            attempt++;
            const backoff = Math.min(60000, 1000 * Math.pow(2, Math.min(attempt, 6)));
            console.log(`Reconnect attempt ${attempt} in ${backoff}ms`);
            await new Promise((r) => setTimeout(r, backoff));
        }
    }
}

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('\nReceived SIGINT. Shutting down gracefully...');
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\nReceived SIGTERM. Shutting down gracefully...');
    process.exit(0);
});

run().catch((e) => {
    console.error("Fatal error:", e);
    process.exit(1);
});
