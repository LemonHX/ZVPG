// ZFS Verioned PostgreSQL Engine - Init Command

import { Command } from "@cliffy/command";
import { checkCommand, log, runCommand } from "../utils.ts";
import { getConfig, loadConfig } from "../config.ts";

interface InitOptions {
  zfsPool?: string;
  mountDir?: string;
  force?: boolean;
  config?: string;
}

export const initCommand = new Command()
  .description("Initialize ZFS Verioned PostgreSQL Engine")
  .option("--zfs-pool <pool>", "ZFS pool name", { default: "zvpg_pool" })
  .option("--mount-dir <dir>", "Mount directory", { default: "/var/lib/zvpg" })
  .option("--force", "Force initialization even if pool exists")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: InitOptions, ..._args: unknown[]) => {
    await loadConfig(options.config);

    log.info("Initializing ZFS Verioned PostgreSQL Engine...");

    // Check dependencies
    const deps = ["zfs", "psql", "pg_ctl", "createdb", "dropdb"];
    const missingDeps = [];

    for (const dep of deps) {
      if (!(await checkCommand(dep))) {
        missingDeps.push(dep);
      }
    }

    if (missingDeps.length > 0) {
      log.error(`Missing dependencies: ${missingDeps.join(", ")}`);
      Deno.exit(1);
    }

    log.success("All dependencies are available");

    const config = getConfig();
    const zfsPool = options.zfsPool || config.zfsPool;
    const mountDir = options.mountDir || config.mountDir;

    // Check if ZFS pool exists
    const poolExists = await checkZfsPool(zfsPool);

    if (poolExists && !options.force) {
      log.error(
        `ZFS pool ${zfsPool} already exists. Use --force to reinitialize.`,
      );
      Deno.exit(1);
    }

    // Create ZFS pool structure
    await createZfsStructure(zfsPool, mountDir);

    log.success("ZFS Verioned PostgreSQL Engine initialized successfully");
  });

async function checkZfsPool(poolName: string): Promise<boolean> {
  const result = await runCommand("zfs", ["list", poolName], {
    stdout: "null",
    stderr: "null",
  });
  return result.success;
}

async function createZfsStructure(
  poolName: string,
  mountDir: string,
): Promise<void> {
  const config = getConfig();

  // Create main datasets
  const datasets = [
    poolName,
    `${poolName}/${config.dataSubdir}`,
    `${poolName}/${config.clonesSubdir}`,
    `${poolName}/branches`,
  ];

  for (const dataset of datasets) {
    const exists = await runCommand("zfs", ["list", dataset], {
      stdout: "null",
      stderr: "null",
    });

    if (!exists.success) {
      log.info(`Creating dataset: ${dataset}`);
      const result = await runCommand("zfs", ["create", dataset]);

      if (!result.success) {
        log.error(`Failed to create dataset: ${dataset}`);
        throw new Error(`ZFS dataset creation failed: ${result.stderr}`);
      }
    }
  }

  // Set mount points
  const mountPoints = [
    { dataset: poolName, mountpoint: `${mountDir}/${poolName}` },
    {
      dataset: `${poolName}/${config.dataSubdir}`,
      mountpoint: `${mountDir}/${poolName}/${config.dataSubdir}`,
    },
    {
      dataset: `${poolName}/${config.clonesSubdir}`,
      mountpoint: `${mountDir}/${poolName}/${config.clonesSubdir}`,
    },
    {
      dataset: `${poolName}/branches`,
      mountpoint: `${mountDir}/${poolName}/branches`,
    },
  ];

  for (const { dataset, mountpoint } of mountPoints) {
    log.info(`Setting mountpoint for ${dataset}: ${mountpoint}`);
    const result = await runCommand("zfs", [
      "set",
      `mountpoint=${mountpoint}`,
      dataset,
    ]);

    if (!result.success) {
      log.warn(`Failed to set mountpoint for ${dataset}: ${result.stderr}`);
    }
  }

  // Create directories
  await Deno.mkdir(mountDir, { recursive: true });
  await Deno.mkdir(`${mountDir}/${poolName}/${config.socketSubdir}`, {
    recursive: true,
  });
  await Deno.mkdir(config.logDir, { recursive: true });

  log.success("ZFS structure created successfully");
}
