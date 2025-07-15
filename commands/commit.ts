// ZFS Versioned PostgreSQL Engine - Commit Command (Alias for Snapshot)

import { Command } from "@cliffy/command";
import {
  createAction,
  deleteAction,
  infoAction,
  listAction,
} from "./snapshot-helper.ts";

const createCommitCommand = new Command()
  .description("Create a new commit (snapshot)")
  .arguments("<name:string>")
  .option("-m, --message <message>", "Commit message", {
    default: "Manual commit",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (options: { message: string; config?: string }, name: string) => {
      await createAction(name, options);
    },
  );

const removeCommitCommand = new Command()
  .description("Remove a commit (snapshot)")
  .arguments("<name:string>")
  .option("-f, --force", "Force removal")
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (options: { force?: boolean; config?: string }, name: string) => {
      await deleteAction(name, options);
    },
  );

const listCommitsCommand = new Command()
  .description("List commits (snapshots)")
  .option("-f, --format <format>", "Output format (table|json)", {
    default: "table",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { format: string; config?: string }) => {
    await listAction(options, "commits");
  });

const showCommitCommand = new Command()
  .description("Show commit (snapshot) information")
  .arguments("<name:string>")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }, name: string) => {
    await infoAction(name, options, "Commit");
  });

export const commitCommand = new Command()
  .description("Manage commits (snapshots)")
  .action(function () {
    this.showHelp();
  })
  .command("create", createCommitCommand)
  .command("remove", removeCommitCommand)
  .command("list", listCommitsCommand)
  .command("show", showCommitCommand);
