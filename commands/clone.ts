// ZFS Verioned PostgreSQL Engine - Clone Command

import { Command } from "@cliffy/command";
import { Table } from "@cliffy/table";
import {
  formatISOTimestamp,
  getCloneMount,
  getNextPort,
  getZfsCloneName,
  isPortInUse,
  log,
  runCommand,
  snapshotExists,
  validatePort,
} from "../utils.ts";
import { getConfig, loadConfig } from "../config.ts";

const createCloneCommand = new Command()
  .description("Create a new clone")
  .arguments("<snapshot:string>")
  .option("-p, --port <port:number>", "PostgreSQL port for the clone")
  .option("-n, --name <name:string>", "Clone name")
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (
      options: { port?: number; name?: string; config?: string },
      snapshot: string,
    ) => {
      await loadConfig(options.config);

      const config = getConfig();

      // Validate and format snapshot name
      let fullSnapshotName = snapshot;
      if (!snapshot.includes("@")) {
        fullSnapshotName = `${config.zfsPool}/${config.dataSubdir}@${snapshot}`;
      }

      if (!(await snapshotExists(fullSnapshotName))) {
        log.error(`Snapshot does not exist: ${fullSnapshotName}`);
        Deno.exit(1);
      }

      // Get or validate port
      let port = options.port;
      if (!port) {
        try {
          port = await getNextPort();
        } catch (error) {
          log.error(error instanceof Error ? error.message : String(error));
          Deno.exit(1);
        }
      } else {
        if (!validatePort(port)) {
          log.error(
            `Invalid port: ${port}. Must be between ${config.clonePortStart} and ${config.clonePortEnd}`,
          );
          Deno.exit(1);
        }

        if (await isPortInUse(port)) {
          log.error(`Port ${port} is already in use`);
          Deno.exit(1);
        }
      }

      // Check if clone already exists
      const cloneDataset = getZfsCloneName(port);
      const cloneExists = await runCommand("zfs", ["list", cloneDataset], {
        stdout: "null",
        stderr: "null",
      });

      if (cloneExists.success) {
        log.error(`Clone already exists on port ${port}`);
        Deno.exit(1);
      }

      // Create clones directory if it doesn't exist
      const clonesDir = `${config.zfsPool}/${config.clonesSubdir}`;
      const clonesDirExists = await runCommand("zfs", ["list", clonesDir], {
        stdout: "null",
        stderr: "null",
      });

      if (!clonesDirExists.success) {
        log.info(`Creating clones directory: ${clonesDir}`);
        const createResult = await runCommand("zfs", ["create", clonesDir]);
        if (!createResult.success) {
          log.error(
            `Failed to create clones directory: ${createResult.stderr}`,
          );
          Deno.exit(1);
        }
      }

      log.info(`Creating clone from snapshot: ${fullSnapshotName}`);
      log.info(`Clone port: ${port}`);

      // Create clone
      const result = await runCommand("zfs", [
        "clone",
        fullSnapshotName,
        cloneDataset,
      ]);

      if (!result.success) {
        log.error(`Failed to create clone: ${result.stderr}`);
        Deno.exit(1);
      }

      // Set clone properties
      const cloneName = options.name || `clone_${port}`;
      await runCommand("zfs", [
        "set",
        `zvpg:clone_name=${cloneName}`,
        cloneDataset,
      ]);
      await runCommand("zfs", ["set", `zvpg:port=${port}`, cloneDataset]);
      await runCommand("zfs", [
        "set",
        `zvpg:snapshot=${fullSnapshotName}`,
        cloneDataset,
      ]);
      await runCommand("zfs", [
        "set",
        `zvpg:created=${formatISOTimestamp()}`,
        cloneDataset,
      ]);

      const cloneMount = getCloneMount(port);

      log.success(`Clone created successfully: ${cloneName}`);
      log.info(`Clone dataset: ${cloneDataset}`);
      log.info(`Clone mount: ${cloneMount}`);
      log.info(`PostgreSQL port: ${port}`);
    },
  );

const deleteCloneCommand = new Command()
  .description("Delete a clone")
  .arguments("<port:number>")
  .option("-f, --force", "Force deletion")
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (options: { force?: boolean; config?: string }, port: number) => {
      await loadConfig(options.config);

      if (!validatePort(port)) {
        const config = getConfig();
        log.error(
          `Invalid port: ${port}. Must be between ${config.clonePortStart} and ${config.clonePortEnd}`,
        );
        Deno.exit(1);
      }

      const cloneDataset = getZfsCloneName(port);

      const cloneExists = await runCommand("zfs", ["list", cloneDataset], {
        stdout: "null",
        stderr: "null",
      });

      if (!cloneExists.success) {
        log.error(`Clone does not exist on port ${port}`);
        Deno.exit(1);
      }

      // Check if PostgreSQL is running
      if (!options.force && await isPortInUse(port)) {
        log.error(
          `PostgreSQL is running on port ${port}. Stop it first or use --force`,
        );
        Deno.exit(1);
      }

      log.info(`Deleting clone on port ${port}`);

      const result = await runCommand("zfs", ["destroy", cloneDataset]);

      if (result.success) {
        log.success(`Clone deleted successfully: port ${port}`);
      } else {
        log.error(`Failed to delete clone: ${result.stderr}`);
        Deno.exit(1);
      }
    },
  );

const listClonesCommand = new Command()
  .description("List clones")
  .option("-f, --format <format>", "Output format (table|json)", {
    default: "table",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { format: string; config?: string }) => {
    await loadConfig(options.config);

    const config = getConfig();

    log.info(`Listing clones for pool: ${config.zfsPool}`);

    const result = await runCommand("zfs", [
      "list",
      "-t",
      "filesystem",
      "-r",
      `${config.zfsPool}/${config.clonesSubdir}`,
      "-H",
      "-o",
      "name,origin,used,available",
    ]);

    if (!result.success || !result.stdout) {
      log.warn("No clones found");
      return;
    }

    const clones = result.stdout.trim().split("\n").filter(Boolean)
      .filter((line) =>
        !line.startsWith(`${config.zfsPool}/${config.clonesSubdir}\t`)
      );

    if (clones.length === 0) {
      log.warn("No clones found");
      return;
    }

    if (options.format === "json") {
      const cloneData = [];

      for (const clone of clones) {
        const [name, origin, used, available] = clone.split("\t");
        const cloneName = name.split("/").pop() || "";
        const port = cloneName.replace("clone_", "");

        // Get metadata
        const cloneNameResult = await runCommand("zfs", [
          "get",
          "-H",
          "-o",
          "value",
          "zvpg:clone_name",
          name,
        ]);
        const displayName = cloneNameResult.success
          ? (cloneNameResult.stdout?.trim() || cloneName)
          : cloneName;

        const createdResult = await runCommand("zfs", [
          "get",
          "-H",
          "-o",
          "value",
          "zvpg:created",
          name,
        ]);
        const created = createdResult.success
          ? (createdResult.stdout?.trim() || "")
          : "";

        const isRunning = await isPortInUse(parseInt(port));

        cloneData.push({
          name: displayName,
          port: parseInt(port),
          origin,
          used,
          available,
          created,
          status: isRunning ? "running" : "stopped",
        });
      }

      console.log(JSON.stringify(cloneData, null, 2));
    } else {
      const table = new Table()
        .header(["Name", "Port", "Origin", "Used", "Status", "Created"])
        .border(true);

      for (const clone of clones) {
        const [name, origin, used] = clone.split("\t");
        const cloneName = name.split("/").pop() || "";
        const port = cloneName.replace("clone_", "");

        // Get metadata
        const cloneNameResult = await runCommand("zfs", [
          "get",
          "-H",
          "-o",
          "value",
          "zvpg:clone_name",
          name,
        ]);
        const displayName = cloneNameResult.success
          ? (cloneNameResult.stdout?.trim() || cloneName)
          : cloneName;

        const createdResult = await runCommand("zfs", [
          "get",
          "-H",
          "-o",
          "value",
          "zvpg:created",
          name,
        ]);
        const created = createdResult.success
          ? (createdResult.stdout?.trim() || "")
          : "";

        const isRunning = await isPortInUse(parseInt(port));
        const status = isRunning ? "Running" : "Stopped";

        // Shorten origin for display
        const shortOrigin = origin.split("@")[1] || origin;

        table.push([displayName, port, shortOrigin, used, status, created]);
      }

      table.render();
    }
  });

const cloneInfoCommand = new Command()
  .description("Show clone information")
  .arguments("<port:number>")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }, port: number) => {
    await loadConfig(options.config);

    if (!validatePort(port)) {
      const config = getConfig();
      log.error(
        `Invalid port: ${port}. Must be between ${config.clonePortStart} and ${config.clonePortEnd}`,
      );
      Deno.exit(1);
    }

    const cloneDataset = getZfsCloneName(port);

    const cloneExists = await runCommand("zfs", ["list", cloneDataset], {
      stdout: "null",
      stderr: "null",
    });

    if (!cloneExists.success) {
      log.error(`Clone does not exist on port ${port}`);
      Deno.exit(1);
    }

    // Get clone information
    const infoResult = await runCommand("zfs", [
      "list",
      "-H",
      "-o",
      "name,origin,used,available,referenced,compressratio,creation",
      cloneDataset,
    ]);

    if (!infoResult.success || !infoResult.stdout) {
      log.error("Failed to get clone information");
      Deno.exit(1);
    }

    const [, origin, used, available, referenced, compressratio, creation] =
      infoResult.stdout.trim().split("\t");

    // Get metadata
    const cloneNameResult = await runCommand("zfs", [
      "get",
      "-H",
      "-o",
      "value",
      "zvpg:clone_name",
      cloneDataset,
    ]);
    const cloneName = cloneNameResult.success
      ? (cloneNameResult.stdout?.trim() || `clone_${port}`)
      : `clone_${port}`;

    const createdResult = await runCommand("zfs", [
      "get",
      "-H",
      "-o",
      "value",
      "zvpg:created",
      cloneDataset,
    ]);
    const created = createdResult.success
      ? (createdResult.stdout?.trim() || "")
      : "";

    const isRunning = await isPortInUse(port);
    const cloneMount = getCloneMount(port);

    console.log("Clone Information:");
    console.log(`  Name: ${cloneName}`);
    console.log(`  Port: ${port}`);
    console.log(`  Dataset: ${cloneDataset}`);
    console.log(`  Mount: ${cloneMount}`);
    console.log(`  Origin: ${origin}`);
    console.log(`  Used: ${used}`);
    console.log(`  Available: ${available}`);
    console.log(`  Referenced: ${referenced}`);
    console.log(`  Compress Ratio: ${compressratio}`);
    console.log(`  Creation: ${creation}`);
    console.log(`  Created: ${created}`);
    console.log(`  Status: ${isRunning ? "Running" : "Stopped"}`);
  });

const startCloneCommand = new Command()
  .description("Start PostgreSQL on a clone")
  .arguments("<port:number>")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }, port: number) => {
    await loadConfig(options.config);

    if (!validatePort(port)) {
      const config = getConfig();
      log.error(
        `Invalid port: ${port}. Must be between ${config.clonePortStart} and ${config.clonePortEnd}`,
      );
      Deno.exit(1);
    }

    const cloneDataset = getZfsCloneName(port);

    const cloneExists = await runCommand("zfs", ["list", cloneDataset], {
      stdout: "null",
      stderr: "null",
    });

    if (!cloneExists.success) {
      log.error(`Clone does not exist on port ${port}`);
      Deno.exit(1);
    }

    if (await isPortInUse(port)) {
      log.error(`PostgreSQL is already running on port ${port}`);
      Deno.exit(1);
    }

    const config = getConfig();
    const cloneMount = getCloneMount(port);

    log.info(`Starting PostgreSQL on port ${port}`);

    // Start PostgreSQL
    const result = await runCommand(`${config.postgresBinPath}/pg_ctl`, [
      "start",
      "-D",
      cloneMount,
      "-o",
      `-p ${port}`,
      "-l",
      `${config.logDir}/clone_${port}.log`,
    ]);

    if (result.success) {
      log.success(`PostgreSQL started successfully on port ${port}`);
    } else {
      log.error(`Failed to start PostgreSQL: ${result.stderr}`);
      Deno.exit(1);
    }
  });

const stopCloneCommand = new Command()
  .description("Stop PostgreSQL on a clone")
  .arguments("<port:number>")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }, port: number) => {
    await loadConfig(options.config);

    if (!validatePort(port)) {
      const config = getConfig();
      log.error(
        `Invalid port: ${port}. Must be between ${config.clonePortStart} and ${config.clonePortEnd}`,
      );
      Deno.exit(1);
    }

    const cloneDataset = getZfsCloneName(port);

    const cloneExists = await runCommand("zfs", ["list", cloneDataset], {
      stdout: "null",
      stderr: "null",
    });

    if (!cloneExists.success) {
      log.error(`Clone does not exist on port ${port}`);
      Deno.exit(1);
    }

    if (!(await isPortInUse(port))) {
      log.error(`PostgreSQL is not running on port ${port}`);
      Deno.exit(1);
    }

    const config = getConfig();
    const cloneMount = getCloneMount(port);

    log.info(`Stopping PostgreSQL on port ${port}`);

    // Stop PostgreSQL
    const result = await runCommand(`${config.postgresBinPath}/pg_ctl`, [
      "stop",
      "-D",
      cloneMount,
      "-m",
      "fast",
    ]);

    if (result.success) {
      log.success(`PostgreSQL stopped successfully on port ${port}`);
    } else {
      log.error(`Failed to stop PostgreSQL: ${result.stderr}`);
      Deno.exit(1);
    }
  });

export const cloneCommand = new Command()
  .description("Manage clones")
  .command("create", createCloneCommand)
  .command("delete", deleteCloneCommand)
  .command("list", listClonesCommand)
  .command("info", cloneInfoCommand)
  .command("start", startCloneCommand)
  .command("stop", stopCloneCommand);
