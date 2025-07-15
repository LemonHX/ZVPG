// ZFS Verioned PostgreSQL Engine - Snapshot Command

import { Command } from "@cliffy/command";
import { Table } from "@cliffy/table";
import {
  formatISOTimestamp,
  log,
  runCommand,
  snapshotExists,
  validateSnapshotName,
} from "../utils.ts";
import { getConfig, loadConfig } from "../config.ts";

const createSnapshotCommand = new Command()
  .description("Create a new snapshot")
  .arguments("<name:string>")
  .option("-m, --message <message>", "Snapshot message", {
    default: "Manual snapshot",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (options: { message: string; config?: string }, name: string) => {
      await loadConfig(options.config);

      if (!validateSnapshotName(name)) {
        log.error("Invalid snapshot name format");
        Deno.exit(1);
      }

      const config = getConfig();
      const fullSnapshotName = `${config.zfsPool}/${config.dataSubdir}@${name}`;

      // Check if snapshot already exists
      if (await snapshotExists(fullSnapshotName)) {
        log.error(`Snapshot already exists: ${fullSnapshotName}`);
        Deno.exit(1);
      }

      // Check if data directory exists
      const dataExists = await runCommand("zfs", [
        "list",
        `${config.zfsPool}/${config.dataSubdir}`,
      ], {
        stdout: "null",
        stderr: "null",
      });

      if (!dataExists.success) {
        log.error(
          `Data directory does not exist: ${config.zfsPool}/${config.dataSubdir}`,
        );
        Deno.exit(1);
      }

      log.info(`Creating snapshot: ${fullSnapshotName}`);

      // Create snapshot
      const result = await runCommand("zfs", ["snapshot", fullSnapshotName]);

      if (!result.success) {
        log.error(`Failed to create snapshot: ${result.stderr}`);
        Deno.exit(1);
      }

      // Add metadata
      await runCommand("zfs", [
        "set",
        `zvpg:message=${options.message}`,
        fullSnapshotName,
      ]);
      await runCommand("zfs", [
        "set",
        `zvpg:created=${formatISOTimestamp()}`,
        fullSnapshotName,
      ]);

      log.success(`Snapshot created successfully: ${fullSnapshotName}`);
    },
  );

const deleteSnapshotCommand = new Command()
  .description("Delete a snapshot")
  .arguments("<name:string>")
  .option("-f, --force", "Force deletion")
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (options: { force?: boolean; config?: string }, name: string) => {
      await loadConfig(options.config);

      const config = getConfig();
      let fullSnapshotName = name;

      if (!name.includes("@")) {
        fullSnapshotName = `${config.zfsPool}/${config.dataSubdir}@${name}`;
      }

      if (!(await snapshotExists(fullSnapshotName))) {
        log.error(`Snapshot does not exist: ${fullSnapshotName}`);
        Deno.exit(1);
      }

      // Check for clones
      const clonesResult = await runCommand("zfs", [
        "list",
        "-t",
        "filesystem",
        "-r",
        config.zfsPool,
        "-H",
        "-o",
        "name,origin",
      ]);

      if (clonesResult.success && clonesResult.stdout) {
        const clones = clonesResult.stdout.trim().split("\n")
          .filter((line) => line.includes(fullSnapshotName))
          .map((line) => line.split("\t")[0]);

        if (clones.length > 0 && !options.force) {
          log.error(
            `Cannot delete snapshot ${fullSnapshotName} - it has dependent clones:`,
          );
          clones.forEach((clone) => console.log(`  - ${clone}`));
          log.error("Use --force to delete anyway");
          Deno.exit(1);
        }
      }

      log.info(`Deleting snapshot: ${fullSnapshotName}`);

      const result = await runCommand("zfs", ["destroy", fullSnapshotName]);

      if (result.success) {
        log.success(`Snapshot deleted successfully: ${fullSnapshotName}`);
      } else {
        log.error(`Failed to delete snapshot: ${result.stderr}`);
        Deno.exit(1);
      }
    },
  );

const listSnapshotsCommand = new Command()
  .description("List snapshots")
  .option("-f, --format <format>", "Output format (table|json)", {
    default: "table",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { format: string; config?: string }) => {
    await loadConfig(options.config);

    const config = getConfig();

    log.info(`Listing snapshots for pool: ${config.zfsPool}`);

    const result = await runCommand("zfs", [
      "list",
      "-t",
      "snapshot",
      "-r",
      config.zfsPool,
      "-H",
      "-o",
      "name,used,referenced,creation",
    ]);

    if (!result.success || !result.stdout) {
      log.warn("No snapshots found");
      return;
    }

    const snapshots = result.stdout.trim().split("\n").filter(Boolean);

    if (options.format === "json") {
      const snapshotData = [];

      for (const snapshot of snapshots) {
        const [name, used, referenced, creation] = snapshot.split("\t");
        const snapshotName = name.split("@")[1];

        // Get metadata
        const messageResult = await runCommand("zfs", [
          "get",
          "-H",
          "-o",
          "value",
          "zvpg:message",
          name,
        ]);
        const message = messageResult.success
          ? (messageResult.stdout?.trim() || "")
          : "";

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

        snapshotData.push({
          name: snapshotName,
          full_name: name,
          used,
          referenced,
          creation,
          message,
          created,
        });
      }

      console.log(JSON.stringify(snapshotData, null, 2));
    } else {
      const table = new Table()
        .header(["Name", "Used", "Referenced", "Creation", "Message"])
        .border(true);

      for (const snapshot of snapshots) {
        const [name, used, referenced, creation] = snapshot.split("\t");
        const snapshotName = name.split("@")[1];

        // Get message
        const messageResult = await runCommand("zfs", [
          "get",
          "-H",
          "-o",
          "value",
          "zvpg:message",
          name,
        ]);
        const message = messageResult.success
          ? (messageResult.stdout?.trim() || "")
          : "";

        table.push([snapshotName, used, referenced, creation, message]);
      }

      table.render();
    }
  });

const snapshotInfoCommand = new Command()
  .description("Show snapshot information")
  .arguments("<name:string>")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }, name: string) => {
    await loadConfig(options.config);

    const config = getConfig();
    let fullSnapshotName = name;

    if (!name.includes("@")) {
      fullSnapshotName = `${config.zfsPool}/${config.dataSubdir}@${name}`;
    }

    if (!(await snapshotExists(fullSnapshotName))) {
      log.error(`Snapshot does not exist: ${fullSnapshotName}`);
      Deno.exit(1);
    }

    // Get snapshot information
    const infoResult = await runCommand("zfs", [
      "list",
      "-t",
      "snapshot",
      "-H",
      "-o",
      "name,used,available,referenced,compressratio,creation",
      fullSnapshotName,
    ]);

    if (!infoResult.success || !infoResult.stdout) {
      log.error("Failed to get snapshot information");
      Deno.exit(1);
    }

    const [, used, available, referenced, compressratio, creation] = infoResult
      .stdout.trim().split("\t");

    // Get metadata
    const messageResult = await runCommand("zfs", [
      "get",
      "-H",
      "-o",
      "value",
      "zvpg:message",
      fullSnapshotName,
    ]);
    const message = messageResult.success
      ? (messageResult.stdout?.trim() || "")
      : "";

    const createdResult = await runCommand("zfs", [
      "get",
      "-H",
      "-o",
      "value",
      "zvpg:created",
      fullSnapshotName,
    ]);
    const created = createdResult.success
      ? (createdResult.stdout?.trim() || "")
      : "";

    // Get clones
    const clonesResult = await runCommand("zfs", [
      "list",
      "-t",
      "filesystem",
      "-r",
      config.zfsPool,
      "-H",
      "-o",
      "name,origin",
    ]);

    const clones: string[] = [];
    if (clonesResult.success && clonesResult.stdout) {
      clonesResult.stdout.trim().split("\n").forEach((line) => {
        const [cloneName, origin] = line.split("\t");
        if (origin === fullSnapshotName) {
          clones.push(cloneName);
        }
      });
    }

    console.log("Snapshot Information:");
    console.log(`  Name: ${name}`);
    console.log(`  Full Name: ${fullSnapshotName}`);
    console.log(`  Used: ${used}`);
    console.log(`  Available: ${available}`);
    console.log(`  Referenced: ${referenced}`);
    console.log(`  Compress Ratio: ${compressratio}`);
    console.log(`  Creation: ${creation}`);
    console.log(`  Message: ${message}`);
    console.log(`  Created: ${created}`);

    if (clones.length > 0) {
      console.log("  Clones:");
      clones.forEach((clone) => console.log(`    - ${clone}`));
    } else {
      console.log("  Clones: None");
    }
  });

export const snapshotCommand = new Command()
  .description("Manage snapshots")
  .command("create", createSnapshotCommand)
  .command("delete", deleteSnapshotCommand)
  .command("list", listSnapshotsCommand)
  .command("info", snapshotInfoCommand);
