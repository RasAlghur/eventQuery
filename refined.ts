import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Contract, WebSocketProvider, JsonRpcProvider, ethers } from 'ethers';
import { TokensGateway } from 'src/app/tokens/tokens.gateway';
import { TokensService } from 'src/app/tokens/tokens.service';
import { TransactionType } from 'src/app/transaction/entities';
import { TransactionService, VolumeService } from 'src/app/transaction/service';
import { UserService } from 'src/app/user/user.service';
import * as fs from 'fs/promises';
import * as path from 'path';

const launcherAbi = [
    'event TokenDeployed(address indexed token, address creator, string myIndex)',
    'event Trade(address indexed user, address indexed token, bool buy, uint256 inAmt, uint256 outAmt)',
    'function getAmountOut(address tok, uint256 amountIn, bool isBuy) external view returns (uint256)',
    'function getCurrentTokenPrice(address tok) external view returns (uint256 priceInETH, uint256 priceInUSD)',
];

const priceFeedAbi = [
    'function getLatestETHPrice(address _priceFeed) external view returns (uint256)',
];

const erc20Abi = [
    'function totalSupply() external view returns (uint256)',
    'function decimals() external view returns (uint8)',
];

interface ContractConfig {
    address: string;
    provider: WebSocketProvider;
    readProvider: JsonRpcProvider;
    tokenVersion: string;
    chainName: string;
    chainId: number;
    isTestnet?: boolean;
}

@Injectable()
export class TransactionEventService implements OnModuleInit, OnModuleDestroy {
    private readonly contracts: Map<string, ContractConfig> = new Map();
    private readonly priceFeed: Contract;
    private readonly bnbPriceFeed: Contract;
    private readonly providerPriceFeed: JsonRpcProvider;
    private readonly bnbProviderPriceFeed: JsonRpcProvider;
    private isShuttingDown = false;
    private reconnectAttempts = new Map<string, number>();

    // State management
    private readonly STATE_DIR = path.resolve(process.cwd(), '.listener_state');
    private readonly PROCESSED_FILE = path.join(this.STATE_DIR, 'processed.jsonl');

    constructor(
        private readonly tokenService: TokensService,
        private readonly transactionService: TransactionService,
        private readonly userService: UserService,
        private readonly volumeService: VolumeService,
        private readonly tokenGateway: TokensGateway,
        private configService: ConfigService,
    ) {
        this.providerPriceFeed = new JsonRpcProvider(
            this.configService.getOrThrow('ALCHEMY_HTTP'),
        );

        this.bnbProviderPriceFeed = new JsonRpcProvider(
            this.configService.getOrThrow('BNB_PRICE_FEED_PROVIDER'),
        );

        this.priceFeed = new Contract(
            this.configService.getOrThrow('PRICE_FEED_ADDRESS'),
            priceFeedAbi,
            this.providerPriceFeed,
        );

        this.bnbPriceFeed = new Contract(
            this.configService.getOrThrow('BNB_PRICE_FEED_ADDRESS'),
            priceFeedAbi,
            this.bnbProviderPriceFeed,
        );

        // Initialize contract configurations
        this.initializeContracts();
    }

    private initializeContracts() {
        const contracts = [
            {
                key: 'v1',
                address: this.configService.getOrThrow('LAUNCHER_ADDRESS_V1'),
                wsUrl: this.configService.getOrThrow('INFURA_WS_V1'),
                httpUrl: this.configService.getOrThrow('INFURA_RPC_V1'),
                tokenVersion: 'token_v1',
                chainName: 'ETH',
                chainId: 1
            },
            {
                key: 'v2',
                address: this.configService.getOrThrow('LAUNCHER_ADDRESS_V2'),
                wsUrl: this.configService.getOrThrow('INFURA_WS_V2'),
                httpUrl: this.configService.getOrThrow('INFURA_RPC_V2'),
                tokenVersion: 'token_v2',
                chainName: 'ETH',
                chainId: 1
            },
            {
                key: 'v3',
                address: this.configService.getOrThrow('LAUNCHER_ADDRESS_V3'),
                wsUrl: this.configService.getOrThrow('INFURA_WS_V3'),
                httpUrl: this.configService.getOrThrow('INFURA_RPC_V3'),
                tokenVersion: 'token_v3',
                chainName: 'ETH',
                chainId: 1
            },
            {
                key: 'v4',
                address: this.configService.getOrThrow('LAUNCHER_ADDRESS_V4'),
                wsUrl: this.configService.getOrThrow('INFURA_WS_V4'),
                httpUrl: this.configService.getOrThrow('INFURA_RPC_V4'),
                tokenVersion: 'token_v4',
                chainName: 'ETH',
                chainId: 1
            },
            {
                key: 'bnb',
                address: this.configService.getOrThrow('BNB_LAUNCHER_ADDRESS_V1'),
                wsUrl: this.configService.getOrThrow('BNB_WS_PROVIDER'),
                httpUrl: this.configService.getOrThrow('BNB_PROVIDER'),
                tokenVersion: 'token_v4',
                chainName: 'BNB',
                chainId: 56
            }
        ];

        for (const contract of contracts) {
            try {
                const wsProvider = new WebSocketProvider(contract.wsUrl);
                const httpProvider = new JsonRpcProvider(contract.httpUrl);
                
                this.contracts.set(contract.key, {
                    address: contract.address,
                    provider: wsProvider,
                    readProvider: httpProvider,
                    tokenVersion: contract.tokenVersion,
                    chainName: contract.chainName,
                    chainId: contract.chainId
                });
                
                this.reconnectAttempts.set(contract.key, 0);
            } catch (error) {
                console.error(`Failed to initialize provider for ${contract.key}:`, error);
            }
        }
    }

    async onModuleInit() {
        console.log('Starting robust event listeners...');
        await this.ensureStateDir();
        this.startEventListeners().catch((err) => {
            console.error('Failed to start event listeners:', err);
        });
    }

    async onModuleDestroy() {
        console.log('Shutting down event listeners...');
        this.isShuttingDown = true;
        
        for (const [key, config] of this.contracts) {
            try {
                await config.provider.destroy();
                console.log(`Disconnected provider for ${key}`);
            } catch (error) {
                console.error(`Error disconnecting provider for ${key}:`, error);
            }
        }
    }

    private async ensureStateDir(): Promise<void> {
        try {
            await fs.mkdir(this.STATE_DIR, { recursive: true });
        } catch {
            /* ignore */
        }
    }

    private async markProcessed(txHash: string, logIndex: number) {
        const line = `${txHash}:${logIndex}\n`;
        await fs.appendFile(this.PROCESSED_FILE, line);
    }

    private async alreadyProcessed(txHash: string, logIndex: number): Promise<boolean> {
        try {
            const raw = await fs.readFile(this.PROCESSED_FILE, 'utf8');
            return raw.includes(`${txHash}:${logIndex}`);
        } catch {
            return false;
        }
    }

    private async startEventListeners() {
        for (const [key, config] of this.contracts) {
            this.startContractListener(key, config).catch((error) => {
                console.error(`Failed to start listener for ${key}:`, error);
            });
        }
    }

    private async startContractListener(key: string, config: ContractConfig) {
        let attempt = 0;
        
        while (!this.isShuttingDown) {
            try {
                console.log(`Starting listener for ${key} (attempt ${attempt + 1})`);
                
                const contract = new Contract(config.address, launcherAbi, config.provider);
                
                // Set up provider error handling
                config.provider.on('error', (error) => {
                    console.error(`Provider error for ${key}:`, error);
                });

                // Test connection
                await config.provider.getBlockNumber();
                console.log(`Connected to ${key} successfully`);

                // Reset attempt counter on successful connection
                attempt = 0;
                this.reconnectAttempts.set(key, 0);

                // Set up event listeners
                await this.setupEventListeners(key, contract, config);

                console.log(`Subscribed to events for ${key}`);

                // Wait for disconnection
                await new Promise<void>((resolve) => {
                    const healthCheck = setInterval(async () => {
                        try {
                            await config.provider.getBlockNumber();
                        } catch (error) {
                            console.error(`Health check failed for ${key}:`, error);
                            clearInterval(healthCheck);
                            resolve();
                        }
                    }, 30000);

                    // Cleanup on shutdown
                    const shutdownCheck = setInterval(() => {
                        if (this.isShuttingDown) {
                            clearInterval(healthCheck);
                            clearInterval(shutdownCheck);
                            resolve();
                        }
                    }, 1000);
                });

                console.log(`Disconnected from ${key}, reconnecting...`);

            } catch (error) {
                console.error(`Listener error for ${key}:`, error);
                
                attempt++;
                this.reconnectAttempts.set(key, attempt);
                
                const backoff = Math.min(60000, 1000 * Math.pow(2, Math.min(attempt, 6)));
                console.log(`Reconnect attempt ${attempt} for ${key} in ${backoff}ms`);
                
                await new Promise(resolve => setTimeout(resolve, backoff));
            }
        }
    }

    private async setupEventListeners(key: string, contract: Contract, config: ContractConfig) {
        // TokenDeployed event
        contract.on('TokenDeployed', async (tokenAddr, creator, myIndex, event) => {
            try {
                const txHash = event.log.transactionHash;
                const logIndex = event.log.logIndex;

                if (await this.alreadyProcessed(txHash, logIndex)) {
                    return;
                }

                console.log(`TokenDeployed event on ${key}:`, event);
                
                const data = await this.tokenService.update(
                    {
                        tokenAddress: tokenAddr,
                        tokenCreator: creator,
                        transactionHash: txHash,
                        tokenVersion: config.tokenVersion,
                        chainName: config.chainName,
                        chainId: config.chainId,
                    },
                    myIndex,
                );

                const savedata = {
                    name: data?.name,
                    symbol: data?.symbol,
                    tokenAddress: data?.tokenAddress,
                    identifier: data?.identifier,
                    creator: data?.tokenCreator,
                    eventType: 'deployment',
                    timestamp: new Date(),
                };

                this.volumeService.saveToFile(savedata);
                this.tokenGateway.broadcastEvent('token_deployment', data);

                await this.markProcessed(txHash, logIndex);

                setTimeout(async () => {
                    try {
                        await this.volumeService.verifyTransaction(tokenAddr, myIndex);
                    } catch (error) {
                        console.error(`Error verifying transaction for ${key}:`, error);
                    }
                }, 120000);

            } catch (error) {
                console.error(`Error handling TokenDeployed event for ${key}:`, error);
            }
        });

        // Trade event
        contract.on('Trade', async (user, tokenAddr, buy, inAmt, outAmt, event) => {
            try {
                const txHash = event.log.transactionHash;
                const logIndex = event.log.logIndex;

                if (await this.alreadyProcessed(txHash, logIndex)) {
                    return;
                }

                console.log(`Trade event on ${key}:`, event);

                let oldMarketCap: number;
                if (config.chainName === 'BNB') {
                    oldMarketCap = await this.handleTokenBNB(
                        tokenAddr,
                        contract,
                        config.readProvider,
                    );
                } else {
                    oldMarketCap = await this.handleToken(
                        tokenAddr,
                        contract,
                        config.readProvider,
                    );
                }

                await this.saveTrade(
                    user,
                    tokenAddr,
                    buy,
                    inAmt,
                    outAmt,
                    oldMarketCap,
                    txHash,
                    event.log.blockNumber,
                    config.readProvider,
                );

                await this.markProcessed(txHash, logIndex);

            } catch (error) {
                console.error(`Error handling Trade event for ${key}:`, error);
            }
        });
    }

    private async saveTrade(
        user: string,
        tokenAddr: string,
        buy: boolean,
        inAmt: any,
        outAmt: any,
        oldMarketCap: number,
        transactionHash: string,
        blockNumber: number,
        provider: JsonRpcProvider,
    ) {
        try {
            const data = {
                user,
                tokenAddr,
                buy,
                inAmt: inAmt.toString() / 1e18,
                outAmt: outAmt.toString() / 1e18,
                timestamp: new Date(),
            };

            const timestamp = await this.getBlockWithRetry(blockNumber, provider);

            const action = buy ? 'buy' : 'sell';
            await this.userService.create({ wallet: user });

            const outAmount = outAmt.toString() / 1e18;
            const inAmount = inAmt.toString() / 1e18;
            
            await this.transactionService.create({
                tokenAddress: tokenAddr,
                tokenAmount: buy ? outAmount : inAmount,
                wallet: user,
                type: TransactionType[action],
                ethAmount: buy ? inAmount : outAmount,
                oldMarketCap,
                txnHash: transactionHash,
                bundleIndex: null,
                timestamp,
                isBundleTransaction: false,
                originalTxnHash: '',
            });

            this.volumeService.saveToFile({ ...data, eventType: 'trade' });
        } catch (err) {
            console.error('Error saving trade:', err);
            throw err;
        }
    }

    private async getBlockWithRetry(
        blockNumber: number,
        provider: JsonRpcProvider,
        retries = 3,
        delay = 1000,
    ) {
        try {
            const currentBlock = await provider.getBlock(blockNumber);
            const tradeTime = currentBlock.timestamp * 1000;
            return new Date(tradeTime).toISOString();
        } catch (error) {
            if (retries > 0) {
                console.log('retrying block number....');
                await new Promise((res) => setTimeout(res, delay));
                return this.getBlockWithRetry(blockNumber, provider, retries - 1, delay * 2);
            }
            if (error.code === 'UNKNOWN_ERROR' && error.error?.code === -32000) {
                console.error('Node internal error, try again later');
            }
            throw error;
        }
    }

    private async handleToken(
        tokenAddr: string,
        launcher: Contract,
        provider: JsonRpcProvider,
    ) {
        const token = new ethers.Contract(tokenAddr, erc20Abi, provider);
        const [rawSupply, decimals] = await Promise.all([
            token.totalSupply(),
            token.decimals(),
        ]);
        const supply = Number(ethers.formatUnits(rawSupply, decimals));

        const rawTokenPerETH = await launcher.getAmountOut(
            tokenAddr,
            1000000000000000000n,
            false,
        );

        const tokenPerETH = Number(ethers.formatUnits(rawTokenPerETH, 18));
        const rawETHUSD = await this.priceFeed.getLatestETHPrice(
            this.configService.getOrThrow('PRICE_GETTER_CHAINLINK'),
        );
        const ethUsd = Number(ethers.formatUnits(rawETHUSD, 8));
        const priceUSD = ethUsd * tokenPerETH;

        return supply * priceUSD;
    }

    private async handleTokenBNB(
        tokenAddr: string,
        launcher: Contract,
        provider: JsonRpcProvider,
    ) {
        const token = new ethers.Contract(tokenAddr, erc20Abi, provider);
        const [rawSupply, decimals] = await Promise.all([
            token.totalSupply(),
            token.decimals(),
        ]);
        const supply = Number(ethers.formatUnits(rawSupply, decimals));

        const rawTokenPerETH = await launcher.getAmountOut(
            tokenAddr,
            1000000000000000000n,
            false,
        );

        const tokenPerETH = Number(ethers.formatUnits(rawTokenPerETH, 18));
        const rawETHUSD = await this.bnbPriceFeed.getLatestETHPrice(
            this.configService.getOrThrow('BNB_PRICE_FEED_ADDRESS'),
        );
        const ethUsd = Number(ethers.formatUnits(rawETHUSD, 8));
        const priceUSD = ethUsd * tokenPerETH;

        return supply * priceUSD;
    }
}
