// ZFS Versioned PostgreSQL Engine - Branch Command

import { Command } from "@cliffy/command";
import { Table } from "@cliffy/table";
import { log } from "../utils.ts";
import { loadConfig } from "../config.ts";
import { BranchService } from "../services/branch.ts";

const createBranchCommand = new Command()
  .description("Create a new branch from current state or snapshot")
  .arguments("<name:string>")
  .option("-f, --from <snapshot>", "Create branch from specific snapshot")
  .option("-p, --parent <parent>", "Parent branch name", { default: "main" })
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (
      options: { from?: string; parent: string; config?: string },
      name: string,
    ) => {
      await loadConfig(options.config);
      const branchService = new BranchService();

      try {
        await branchService.createBranch(name, options.from, options.parent);
        log.success(`Branch '${name}' created successfully`);
      } catch (error) {
        log.error(error instanceof Error ? error.message : String(error));
        Deno.exit(1);
      }
    },
  );

const deleteBranchCommand = new Command()
  .description("Delete a branch")
  .arguments("<name:string>")
  .option("-f, --force", "Force deletion")
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (options: { force?: boolean; config?: string }, name: string) => {
      await loadConfig(options.config);
      const branchService = new BranchService();

      try {
        await branchService.deleteBranch(name, options.force || false);
        log.success(`Branch '${name}' deleted successfully`);
      } catch (error) {
        log.error(error instanceof Error ? error.message : String(error));
        Deno.exit(1);
      }
    },
  );

const listBranchesCommand = new Command()
  .description("List all branches")
  .option("-f, --format <format>", "Output format (table|json)", {
    default: "table",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { format: string; config?: string }) => {
    await loadConfig(options.config);
    const branchService = new BranchService();

    try {
      const branches = await branchService.listBranches();

      if (options.format === "json") {
        console.log(JSON.stringify(branches, null, 2));
      } else {
        if (branches.length === 0) {
          log.info("No branches found");
          return;
        }

        const table = new Table()
          .header([
            "Name",
            "Parent Branch",
            "Parent Snapshot",
            "Used",
            "Created",
          ])
          .body(
            branches.map((branch) => [
              branch.name,
              branch.parentBranch,
              branch.parentSnapshot.split("@")[1] || branch.parentSnapshot,
              branch.used,
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

const branchInfoCommand = new Command()
  .description("Show branch information")
  .arguments("<name:string>")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }, name: string) => {
    await loadConfig(options.config);
    const branchService = new BranchService();

    try {
      const info = await branchService.getBranchInfo(name);

      const table = new Table()
        .header(["Property", "Value"])
        .body([
          ["Branch Name", info.name],
          ["Parent Branch", info.parentBranch],
          ["Parent Snapshot", info.parentSnapshot],
          ["Dataset", info.dataset],
          ["Mount Point", info.mount],
          ["Used", info.used],
          ["Available", info.available],
          ["Referenced", info.referenced],
          ["Compression Ratio", info.compressratio],
          ["Created", info.created],
          ["Clones", info.clones.length > 0 ? info.clones.join(", ") : "None"],
        ]);

      console.log(table.toString());
    } catch (error) {
      log.error(error instanceof Error ? error.message : String(error));
      Deno.exit(1);
    }
  });

const branchSnapshotCommand = new Command()
  .description("Create a snapshot from a branch")
  .arguments("<branch:string> <snapshot:string>")
  .option("-m, --message <message>", "Snapshot message", {
    default: "Branch snapshot",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (
      options: { message: string; config?: string },
      branch: string,
      snapshot: string,
    ) => {
      await loadConfig(options.config);
      const branchService = new BranchService();

      try {
        await branchService.createBranchSnapshot(
          branch,
          snapshot,
          options.message,
        );
        log.success(
          `Snapshot '${snapshot}' created successfully from branch '${branch}'`,
        );
      } catch (error) {
        log.error(error instanceof Error ? error.message : String(error));
        Deno.exit(1);
      }
    },
  );

export const branchCommand = new Command()
  .description("Manage branches")
  .command("create", createBranchCommand)
  .command("delete", deleteBranchCommand)
  .command("list", listBranchesCommand)
  .command("info", branchInfoCommand)
  .command("snapshot", branchSnapshotCommand);
