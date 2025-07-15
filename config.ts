// ZFS Verioned PostgreSQL Engine - Configuration Management

export interface Config {
  zfsPool: string;
  mountDir: string;
  dataSubdir: string;
  clonesSubdir: string;
  socketSubdir: string;

  postgresUser: string;
  postgresDb: string;
  postgresVersion: string;
  postgresBinPath: string;
  postgresService: string;

  clonePortStart: number;
  clonePortEnd: number;
  cloneAccessHost: string;
  maxClones: number;
  cloneIdleTimeout: number;

  snapshotRetention: number;
  preSnapshotSuffix: string;
  snapshotInterval: number;

  branchDefault: string;
  branchNamingPattern: string;

  logLevel: "DEBUG" | "INFO" | "WARN" | "ERROR";
  logDir: string;
}

export const defaultConfig: Config = {
  zfsPool: "zvpg_pool",
  mountDir: "/var/lib/zvpg",
  dataSubdir: "data",
  clonesSubdir: "clones",
  socketSubdir: "sockets",

  postgresUser: "postgres",
  postgresDb: "postgres",
  postgresVersion: "17",
  postgresBinPath: "/usr/lib/postgresql/17/bin",
  postgresService: "postgresql",

  clonePortStart: 6001,
  clonePortEnd: 6099,
  cloneAccessHost: "127.0.0.1",
  maxClones: 10,
  cloneIdleTimeout: 300,

  snapshotRetention: 24,
  preSnapshotSuffix: "_pre",
  snapshotInterval: 3600,

  branchDefault: "main",
  branchNamingPattern: "^[a-zA-Z0-9_-]+$",

  logLevel: "INFO",
  logDir: "/var/log/zvpg",
};

let config: Config = defaultConfig;

export function getConfig(): Config {
  return config;
}

export async function loadConfig(
  configPath?: string,
): Promise<void> {
  const cp = configPath ?? "~/.zvpg/config.json";
  try {
    const configText = await Deno.readTextFile(cp);
    const userConfig = JSON.parse(configText);
    config = { ...defaultConfig, ...userConfig };
  } catch (error) {
    if (error instanceof Deno.errors.NotFound) {
      // Use default config if file doesn't exist
      config = defaultConfig;
    } else {
      throw error;
    }
  }
}

export function setConfig(newConfig: Partial<Config>): void {
  config = { ...config, ...newConfig };
}
