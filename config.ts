// ZFS Verioned PostgreSQL Engine - Configuration Management

export interface Config {
  zfsPool: string;
  mountDir: string;
  dataSubdir: string;
  clonesSubdir: string;
  socketSubdir: string;

  postgresUser: string;
  postgresDb: string;

  // Container configuration
  containerRuntime: "docker" | "podman" | "nerdctl";
  pgBaseImage: string;
  pgConfigDir: string;
  pgHbaPath: string;
  pgConfPath: string;
  pgIdentPath: string;

  branchPortStart: number;
  branchPortEnd: number;

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

  containerRuntime: "docker",
  pgBaseImage: "postgres:17",
  pgConfigDir: "~/.zvpg",
  pgHbaPath: "~/.zvpg/pg_hba.conf",
  pgConfPath: "~/.zvpg/postgresql.conf",
  pgIdentPath: "~/.zvpg/pg_ident.conf",

  branchPortStart: 6001,
  branchPortEnd: 6099,

  branchDefault: "main",
  branchNamingPattern:
    "^(?!\\/|\\.|.*([\\/\\.]\\.|\\\/\\\/|\\.lock$))[\\p{L}\\p{N}\\-_\\/]+$",

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
