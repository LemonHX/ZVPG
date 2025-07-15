// ZFS Versioned PostgreSQL Engine - Snapshot Command Helper
// Common action logic for snapshot and commit commands

import { Table } from "@cliffy/table";
import { log } from "../utils.ts";
import { loadConfig } from "../config.ts";
import { SnapshotService } from "../services/snapshot.ts";

export interface CreateActionOptions {
  message: string;
  config?: string;
}

export interface DeleteActionOptions {
  force?: boolean;
  config?: string;
}

export interface ListActionOptions {
  format: string;
  config?: string;
}

export interface InfoActionOptions {
  config?: string;
}

/**
 * Common action for creating snapshots/commits
 */
export async function createAction(
  name: string,
  options: CreateActionOptions,
): Promise<void> {
  await loadConfig(options.config);
  const snapshotService = new SnapshotService();

  try {
    await snapshotService.createSnapshot(name, options.message);
  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    Deno.exit(1);
  }
}

/**
 * Common action for deleting snapshots/commits
 */
export async function deleteAction(
  name: string,
  options: DeleteActionOptions,
): Promise<void> {
  await loadConfig(options.config);
  const snapshotService = new SnapshotService();

  try {
    await snapshotService.deleteSnapshot(name, options.force || false);
  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    Deno.exit(1);
  }
}

/**
 * Common action for listing snapshots/commits
 */
export async function listAction(
  options: ListActionOptions,
  entityType: "snapshots" | "commits" = "snapshots",
): Promise<void> {
  await loadConfig(options.config);
  const snapshotService = new SnapshotService();

  try {
    const items = await snapshotService.listSnapshots();

    if (items.length === 0) {
      log.warn(`No ${entityType} found`);
      return;
    }

    if (options.format === "json") {
      const itemData = items.map((item) => ({
        name: item.name,
        full_name: item.fullName,
        used: item.used,
        available: item.available,
        referenced: item.referenced,
        compressratio: item.compressratio,
        creation: item.creation,
        message: item.message,
        created: item.created,
        clones: item.clones,
      }));

      console.log(JSON.stringify(itemData, null, 2));
    } else {
      const table = new Table()
        .header(["Name", "Used", "Referenced", "Creation", "Message"])
        .border(true);

      items.forEach((item) => {
        table.push([
          item.name,
          item.used,
          item.referenced,
          item.creation,
          item.message,
        ]);
      });

      table.render();
    }
  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    Deno.exit(1);
  }
}

/**
 * Common action for showing snapshot/commit information
 */
export async function infoAction(
  name: string,
  options: InfoActionOptions,
  entityType: "Snapshot" | "Commit" = "Snapshot",
): Promise<void> {
  await loadConfig(options.config);
  const snapshotService = new SnapshotService();

  try {
    const item = await snapshotService.getSnapshotInfo(name);

    console.log(`${entityType} Information:`);
    console.log(`  Name: ${item.name}`);
    console.log(`  Full Name: ${item.fullName}`);
    console.log(`  Used: ${item.used}`);
    console.log(`  Available: ${item.available}`);
    console.log(`  Referenced: ${item.referenced}`);
    console.log(`  Compress Ratio: ${item.compressratio}`);
    console.log(`  Creation: ${item.creation}`);
    console.log(`  Message: ${item.message}`);
    console.log(`  Created: ${item.created}`);

    if (item.clones.length > 0) {
      console.log("  Clones:");
      item.clones.forEach((clone) => console.log(`    - ${clone}`));
    } else {
      console.log("  Clones: None");
    }
  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    Deno.exit(1);
  }
}
