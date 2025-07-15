import { Command } from "@cliffy/command";
import { StatusService, SystemStatus } from "../services/status.ts";
import { getConfig, loadConfig } from "../config.ts";
import { log } from "../utils.ts";
import { Table } from "@cliffy/table";
import { colors } from "@cliffy/ansi/colors";

export const statusCommand = new Command()
  .name("status")
  .description("Show the status of the sudobase system.")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }) => {
    try {
      await loadConfig(options.config);
      const config = await getConfig();
      const statusService = new StatusService(config);
      const status = await statusService.getSystemStatus();
      displayStatus(status);
    } catch (error) {
      log.error(`Error getting status: ${(error as Error).message}`);
    }
  });

function displayStatus(status: SystemStatus) {
  console.log(colors.bold.underline("Sudobase System Status"));
  console.log();

  // System Info
  new Table()
    .header(["System Information"])
    .body([
      [`Hostname: ${status.system.hostname}`],
      [`Uptime: ${status.system.uptime}`],
      [`Load Average: ${status.system.loadAverage}`],
      [`Memory Usage: ${status.system.memoryUsage}`],
      [`Disk Usage: ${status.system.diskUsage}`],
    ])
    .border(true)
    .render();
  console.log();

  // ZFS Pool
  new Table()
    .header(["ZFS Pool Status"])
    .body([
      [`Pool Name: ${status.zfsPool.name}`],
      [`Health: ${
        status.zfsPool.health === "ONLINE"
          ? colors.green(status.zfsPool.health)
          : colors.red(status.zfsPool.health)
      }`],
      [`Size: ${status.zfsPool.size}`],
      [`Used: ${status.zfsPool.used}`],
      [`Available: ${status.zfsPool.available}`],
    ])
    .border(true)
    .render();
  console.log();

  // PostgreSQL
  new Table()
    .header(["PostgreSQL Status"])
    .body([
      [`Version: ${status.postgres.version}`],
      [`Status: ${
        status.postgres.running
          ? colors.green("Running")
          : colors.red("Stopped")
      }`],
      [`Main Port: ${status.postgres.mainPort}`],
    ])
    .border(true)
    .render();
  console.log();

  // Snapshots
  console.log(
    colors.bold(
      `Snapshots (${status.snapshots.total} total, ${status.snapshots.totalSize})`,
    ),
  );
  if (status.snapshots.details.length > 0) {
    const snapshotTable = new Table()
      .header(["Name", "Size", "Referenced", "Created"])
      .body(
        status.snapshots.details.map(
          (s) => [s.name, s.size, s.referenced, s.created],
        ),
      );
    snapshotTable.border(true).render();
  } else {
    console.log("No snapshots found.");
  }
  console.log();

  // Clones
  console.log(
    colors.bold(
      `Clones (${status.clones.total} total, ${status.clones.active} active, ${status.clones.inactive} inactive)`,
    ),
  );
  if (status.clones.details.length > 0) {
    const cloneTable = new Table()
      .header(["Name", "Status", "Port", "Size", "Created"])
      .body(status.clones.details.map((c) => {
        let statusText;
        switch (c.status) {
          case "running":
            statusText = colors.green("Running");
            break;
          case "stopped":
            statusText = colors.red("Stopped");
            break;
          default:
            statusText = colors.yellow("Unknown");
        }
        return [c.name, statusText, c.port.toString(), c.size, c.created];
      }));
    cloneTable.border(true).render();
  } else {
    console.log("No clones found.");
  }
  console.log();
}
