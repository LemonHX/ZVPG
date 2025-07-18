// ZFS Verioned PostgreSQL Engine - Snapshot Command

import { Command } from "@cliffy/command";
import {
  createAction,
  deleteAction,
  infoAction,
  listAction,
} from "./snapshot-helper.ts";

const createSnapshotCommand = new Command()
  .description("Create a new snapshot")
  .arguments("<name:string>")
  .option("-m, --message <message>", "Snapshot message", {
    default: "Manual snapshot",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (options: { message: string; config?: string }, name: string) => {
      await createAction(name, options);
    },
  );

const deleteSnapshotCommand = new Command()
  .description("Delete a snapshot")
  .arguments("<name:string>")
  .option("-f, --force", "Force deletion")
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (options: { force?: boolean; config?: string }, name: string) => {
      await deleteAction(name, options);
    },
  );

const listSnapshotsCommand = new Command()
  .description("List snapshots")
  .option("-f, --format <format>", "Output format (table|json)", {
    default: "table",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { format: string; config?: string }) => {
    await listAction(options, "snapshots");
  });

const snapshotInfoCommand = new Command()
  .description("Show snapshot information")
  .arguments("<name:string>")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }, name: string) => {
    await infoAction(name, options, "Snapshot");
  });

export const snapshotCommand = new Command()
  .description("Manage snapshots")
  .action(function () {
    this.showHelp();
  })
  .command("create", createSnapshotCommand)
  .command("delete", deleteSnapshotCommand)
  .command("list", listSnapshotsCommand)
  .command("info", snapshotInfoCommand);
