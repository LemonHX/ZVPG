#!/usr/bin/env -S deno run --allow-all
// ZFS Verioned PostgreSQL Engine - Main CLI Entry Point

import { Command } from "@cliffy/command";
import { version } from "./version.ts";
import { initCommand } from "./commands/init.ts";
import { statusCommand } from "./commands/status.ts";
import { snapshotCommand } from "./commands/snapshot.ts";
import { commitCommand } from "./commands/commit.ts";
import { branchCommand } from "./commands/branch.ts";

const logo = `
╔══════════════╗ ███████╗██╗   ██╗██████╗  ██████╗ 
║ ██  ██  ████ ║ ╚══███╔╝██║   ██║██╔══██╗██╔════╝ 
║ ███ █ ██████ ║   ███╔╝ ██║   ██║██████╔╝██║  ███╗
║ ███  ███████ ║  ███╔╝  ██║   ██║██╔═══╝ ██║   ██║
║ ██   ███████ ║ ███████╗╚█████╔╝ ██║     ╚██████╔╝
╚══════════════╝ ╚══════╝ ╚═══╝   ╚═╝      ╚═════╝ 
`;

function renderLogo(): string {
  const lines = logo.split("\n");
  const width = Math.max(...lines.map((line) => line.length));
  let output = "";

  for (const line of lines) {
    for (let i = 0; i < line.length; i++) {
      const ratio = i / width;
      const r = Math.floor(255 - (255 * ratio));
      const g = Math.floor(200 * ratio);
      const b = Math.floor(255 - (55 * ratio));
      output += `\x1b[38;2;${r};${g};${b}m${line[i]}`;
    }
    output += "\x1b[0m\n";
  }

  return output;
}

const cli = new Command()
  .name("zvpg")
  .version(version)
  .description("ZFS Verioned PostgreSQL Engine - PostgreSQL Branch Management")
  .action(function () {
    console.log("\n" + renderLogo());
    this.showHelp();
  })
  .command("init", initCommand)
  .command("commit", commitCommand)
  .command("branch", branchCommand)
  .command("status", statusCommand)
  .command("snapshot", snapshotCommand);

await cli.parse(Deno.args);
