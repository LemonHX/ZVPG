// ZFS Versioned PostgreSQL Engine - Clone Command

import { Command } from "@cliffy/command";
import {
  createCloneAction,
  deleteCloneAction,
  infoCloneAction,
  listCloneAction,
  startStopCloneAction,
} from "./clone-helper.ts";

const createCloneCommand = new Command()
  .description("Create a new clone from a snapshot")
  .arguments("<snapshot:string> <clone:string>")
  .option(
    "-p, --port <port:number>",
    "Port for the cloned PostgreSQL instance",
  )
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (
      options: { port?: number; config?: string },
      snapshot: string,
      clone: string,
    ) => {
      await createCloneAction(snapshot, clone, options);
    },
  );

const deleteCloneCommand = new Command()
  .description("Delete a clone")
  .arguments("<name:string>")
  .option("-f, --force", "Force deletion")
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (options: { force?: boolean; config?: string }, name: string) => {
      await deleteCloneAction(name, options);
    },
  );

const listClonesCommand = new Command()
  .description("List all clones")
  .option("-f, --format <format>", "Output format (table|json)", {
    default: "table",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { format: string; config?: string }) => {
    await listCloneAction(options);
  });

const cloneInfoCommand = new Command()
  .description("Show clone information")
  .arguments("<name:string>")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }, name: string) => {
    await infoCloneAction(name, options);
  });

const startCloneCommand = new Command()
  .description("Start a clone's PostgreSQL instance")
  .arguments("<name:string>")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }, name: string) => {
    await startStopCloneAction(name, "start", options);
  });

const stopCloneCommand = new Command()
  .description("Stop a clone's PostgreSQL instance")
  .arguments("<name:string>")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }, name: string) => {
    await startStopCloneAction(name, "stop", options);
  });

export const cloneCommand = new Command()
  .description("Manage database clones")
  .action(function () {
    this.showHelp();
  })
  .command("create", createCloneCommand)
  .command("delete", deleteCloneCommand)
  .command("list", listClonesCommand)
  .command("info", cloneInfoCommand)
  .command("start", startCloneCommand)
  .command("stop", stopCloneCommand);
