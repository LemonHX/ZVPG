// ZFS Versioned PostgreSQL Engine - Status Command

import { Command } from "@cliffy/command";
import { Table } from "@cliffy/table";
import { log } from "../utils.ts";
import { getConfig, loadConfig } from "../config.ts";
import { StatusService } from "../services/status.ts";

const systemStatusCommand = new Command()
  .description("Show system status")
  .option("-f, --format <format>", "Output format (table|json)", {
    default: "table",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { format: string; config?: string }) => {
    await loadConfig(options.config);
    const config = getConfig();
    const statusService = new StatusService(config);

    try {
      const status = await statusService.getSystemStatus();

      if (options.format === "json") {
        console.log(JSON.stringify(status, null, 2));
      } else {
        // ZFS Pool Status
        console.log("\n=== ZFS Pool Status ===");
        const zfsTable = new Table()
          .header(["Property", "Value"])
          .body([
            ["Name", status.zfsPool.name],
            ["Health", status.zfsPool.health],
            ["Size", status.zfsPool.size],
            ["Used", status.zfsPool.used],
            ["Available", status.zfsPool.available],
          ]);
        console.log(zfsTable.toString());

        // PostgreSQL Status
        console.log("\n=== PostgreSQL Status ===");
        const pgTable = new Table()
          .header(["Property", "Value"])
          .body([
            ["Version", status.postgres.version],
            ["Running", status.postgres.running ? "Yes" : "No"],
            ["Main Port", status.postgres.mainPort.toString()],
          ]);
        console.log(pgTable.toString());

        // Branches Status
        console.log("\n=== Branches Status ===");
        const branchesTable = new Table()
          .header(["Property", "Value"])
          .body([
            ["Total", status.branches.total.toString()],
            ["Active", status.branches.active.toString()],
            ["Inactive", status.branches.inactive.toString()],
          ]);
        console.log(branchesTable.toString());

        // Snapshots Status
        console.log("\n=== Snapshots Status ===");
        const snapshotsTable = new Table()
          .header(["Property", "Value"])
          .body([
            ["Total", status.snapshots.total.toString()],
            ["Total Size", status.snapshots.totalSize],
          ]);
        console.log(snapshotsTable.toString());

        // System Status
        console.log("\n=== System Status ===");
        const systemTable = new Table()
          .header(["Property", "Value"])
          .body([
            ["Hostname", status.system.hostname],
            ["Uptime", status.system.uptime],
            ["Load Average", status.system.loadAverage],
            ["Memory Usage", status.system.memoryUsage],
            ["Disk Usage", status.system.diskUsage],
          ]);
        console.log(systemTable.toString());
      }
    } catch (error) {
      log.error(error instanceof Error ? error.message : String(error));
      Deno.exit(1);
    }
  });

const branchesStatusCommand = new Command()
  .description("Show detailed branches status")
  .option("-f, --format <format>", "Output format (table|json)", {
    default: "table",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { format: string; config?: string }) => {
    await loadConfig(options.config);
    const config = getConfig();
    const statusService = new StatusService(config);

    try {
      const status = await statusService.getSystemStatus();

      if (options.format === "json") {
        console.log(JSON.stringify(status.branches, null, 2));
      } else {
        if (status.branches.details.length === 0) {
          log.info("No branches found");
          return;
        }

        const table = new Table()
          .header(["Name", "Port", "Status", "Size", "Created"])
          .body(
            status.branches.details.map((branch) => [
              branch.name,
              branch.port ? branch.port.toString() : "N/A",
              branch.status,
              branch.size,
              branch.created,
            ]),
          );

        console.log(table.toString());
      }
    } catch (error) {
      log.error(error instanceof Error ? error.message : String(error));
      Deno.exit(1);
    }
  });

const snapshotsStatusCommand = new Command()
  .description("Show detailed snapshots status")
  .option("-f, --format <format>", "Output format (table|json)", {
    default: "table",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { format: string; config?: string }) => {
    await loadConfig(options.config);
    const config = getConfig();
    const statusService = new StatusService(config);

    try {
      const status = await statusService.getSystemStatus();

      if (options.format === "json") {
        console.log(JSON.stringify(status.snapshots, null, 2));
      } else {
        if (status.snapshots.details.length === 0) {
          log.info("No snapshots found");
          return;
        }

        const table = new Table()
          .header(["Name", "Size", "Referenced", "Created"])
          .body(
            status.snapshots.details.map((snapshot) => [
              snapshot.name,
              snapshot.size,
              snapshot.referenced,
              snapshot.created,
            ]),
          );

        console.log(table.toString());
      }
    } catch (error) {
      log.error(error instanceof Error ? error.message : String(error));
      Deno.exit(1);
    }
  });

const healthCheckCommand = new Command()
  .description("Perform a health check of the system")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }) => {
    await loadConfig(options.config);
    const config = getConfig();
    const statusService = new StatusService(config);

    try {
      const status = await statusService.getSystemStatus();

      log.info("Performing health check...");

      // Basic health checks
      const issues: string[] = [];

      if (status.zfsPool.health !== "ONLINE") {
        issues.push(`ZFS Pool health is ${status.zfsPool.health}`);
      }

      if (!status.postgres.running) {
        issues.push("PostgreSQL is not running");
      }

      if (status.branches.inactive > 0) {
        issues.push(`${status.branches.inactive} branches are inactive`);
      }

      if (issues.length === 0) {
        log.success("System is healthy");
      } else {
        log.error("System health issues detected:");
        issues.forEach((issue: string) => log.warn(`- ${issue}`));
      }
    } catch (error) {
      log.error(error instanceof Error ? error.message : String(error));
      Deno.exit(1);
    }
  });

export const statusCommand = new Command()
  .description("Show system status and health")
  .action(function () {
    this.showHelp();
  })
  .command("system", systemStatusCommand)
  .command("branches", branchesStatusCommand)
  .command("snapshots", snapshotsStatusCommand)
  .command("health", healthCheckCommand);
