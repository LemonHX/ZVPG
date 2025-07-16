#!/usr/bin/env -S deno run --allow-all
// ZFS Verioned PostgreSQL Engine - Main CLI Entry Point

import { Command } from "@cliffy/command";
import { version } from "./version.ts";
import { initCommand } from "./commands/init.ts";
import { statusCommand } from "./commands/status.ts";
import { snapshotCommand } from "./commands/snapshot.ts";
import { commitCommand } from "./commands/commit.ts";
import { branchCommand } from "./commands/branch.ts";

const logo = await Deno.readTextFile(
  new URL("logo_output.txt", import.meta.url),
);

const cli = new Command()
  .name("zvpg")
  .version(version)
  .description("ZFS Verioned PostgreSQL Engine - PostgreSQL Branch Management")
  .action(function () {
    console.log("\n\n" + logo);
    this.showHelp();
  })
  .command("init", initCommand)
  .command("commit", commitCommand)
  .command("branch", branchCommand)
  .command("status", statusCommand)
  .command("snapshot", snapshotCommand);

await cli.parse(Deno.args);
