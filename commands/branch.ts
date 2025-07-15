// ZFS Verioned PostgreSQL Engine - Branch Command

import { Command } from "@cliffy/command";
import { Table } from "@cliffy/table";
import { BranchService } from "../services/branch.ts";
import { loadConfig } from "../config.ts";
import { log } from "../utils.ts";

const branchService = new BranchService();

const createBranchCommand = new Command()
  .description("Create a new branch")
  .arguments("<name:string>")
  .option("-s, --snapshot <snapshot>", "Parent snapshot")
  .option("-p, --parent <parent>", "Parent branch", { default: "main" })
  .option("-c, --config <config>", "Configuration file path")
  .action(
    async (
      options: { snapshot?: string; parent: string; config?: string },
      name: string,
    ) => {
      try {
        await loadConfig(options.config);
        await branchService.createBranch(
          name,
          options.snapshot,
          options.parent,
        );
      } catch (error) {
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        log.error(errorMessage);
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
      try {
        await loadConfig(options.config);
        await branchService.deleteBranch(name, options.force);
      } catch (error) {
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        log.error(errorMessage);
        Deno.exit(1);
      }
    },
  );

const listBranchesCommand = new Command()
  .description("List branches")
  .option("-f, --format <format>", "Output format (table|json)", {
    default: "table",
  })
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { format: string; config?: string }) => {
    try {
      await loadConfig(options.config);
      const branches = await branchService.listBranches();

      if (branches.length === 0) {
        log.warn("No branches found");
        return;
      }

      if (options.format === "json") {
        const branchData = branches.map((branch) => ({
          name: branch.name,
          parent_branch: branch.parentBranch,
          parent_snapshot: branch.parentSnapshot,
          created: branch.created,
          used: branch.used,
          available: branch.available,
          referenced: branch.referenced,
          clone_count: branch.clones.length,
        }));

        console.log(JSON.stringify(branchData, null, 2));
      } else {
        const table = new Table()
          .header(["Branch", "Parent", "Parent Snapshot", "Created", "Clones"])
          .border(true);

        for (const branch of branches) {
          // 只显示快照名称部分
          const shortSnapshot = branch.parentSnapshot.split("@")[1] ||
            branch.parentSnapshot;

          table.push([
            branch.name,
            branch.parentBranch,
            shortSnapshot,
            branch.created,
            branch.clones.length.toString(),
          ]);
        }

        table.render();
      }
    } catch (error) {
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);
      log.error(errorMessage);
      Deno.exit(1);
    }
  });

const branchInfoCommand = new Command()
  .description("Show branch information")
  .arguments("<name:string>")
  .option("-c, --config <config>", "Configuration file path")
  .action(async (options: { config?: string }, name: string) => {
    try {
      await loadConfig(options.config);
      const branch = await branchService.getBranchInfo(name);

      console.log("Branch Information:");
      console.log(`  Name: ${branch.name}`);
      console.log(`  Parent Branch: ${branch.parentBranch}`);
      console.log(`  Parent Snapshot: ${branch.parentSnapshot}`);
      console.log(`  Created: ${branch.created}`);
      console.log(`  Dataset: ${branch.dataset}`);
      console.log(`  Mount: ${branch.mount}`);
      console.log(`  Used: ${branch.used}`);
      console.log(`  Available: ${branch.available}`);
      console.log(`  Referenced: ${branch.referenced}`);
      console.log(`  Compress Ratio: ${branch.compressratio}`);

      if (branch.clones.length > 0) {
        console.log("  Clones:");
        branch.clones.forEach((clone) => console.log(`    - ${clone}`));
      } else {
        console.log("  Clones: None");
      }
    } catch (error) {
      const errorMessage = error instanceof Error
        ? error.message
        : String(error);
      log.error(errorMessage);
      Deno.exit(1);
    }
  });

const branchSnapshotCommand = new Command()
  .description("Create snapshot from branch")
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
      try {
        await loadConfig(options.config);
        await branchService.createBranchSnapshot(
          branch,
          snapshot,
          options.message,
        );
      } catch (error) {
        const errorMessage = error instanceof Error
          ? error.message
          : String(error);
        log.error(errorMessage);
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
