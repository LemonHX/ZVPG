#!/usr/bin/env -S deno run --allow-all
// ZFS Verioned PostgreSQL Engine - Main CLI Entry Point

import { Command } from "@cliffy/command";
import { version } from "./version.ts";
import { initCommand } from "./commands/init.ts";
import { statusCommand } from "./commands/status.ts";
import { snapshotCommand } from "./commands/snapshot.ts";
import { cloneCommand } from "./commands/clone.ts";
import { branchCommand } from "./commands/branch.ts";

const cli = new Command()
  .name("zvpg")
  .version(version)
  .description("ZFS Verioned PostgreSQL Engine - PostgreSQL Clone Management")
  .command("init", initCommand)
  .command("status", statusCommand)
  .command("commit", snapshotCommand)
  .command("snapshot", snapshotCommand)
  .command("clone", cloneCommand)
  .command("branch", branchCommand);

await cli.parse(Deno.args);
