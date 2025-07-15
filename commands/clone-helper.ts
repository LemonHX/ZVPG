// ZFS Versioned PostgreSQL Engine - Clone Command Helper
// Common action logic for clone commands

import { Table } from "@cliffy/table";
import { log } from "../utils.ts";
import { getConfig, loadConfig } from "../config.ts";
import { CloneService } from "../services/clone.ts";

export interface CreateCloneActionOptions {
  port?: number;
  config?: string;
}

export interface DeleteCloneActionOptions {
  force?: boolean;
  config?: string;
}

export interface ListCloneActionOptions {
  format: string;
  config?: string;
}

export interface InfoCloneActionOptions {
  config?: string;
}

/**
 * Common action for creating clones
 */
export async function createCloneAction(
  snapshot: string,
  clone: string,
  options: CreateCloneActionOptions,
): Promise<void> {
  await loadConfig(options.config);
  const config = getConfig();
  const cloneService = new CloneService(config);

  try {
    const port = options.port || 5433;
    await cloneService.createClone(snapshot, clone, port);
    log.success(
      `Clone '${clone}' created successfully from snapshot '${snapshot}' on port ${port}`,
    );
  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    Deno.exit(1);
  }
}

/**
 * Common action for deleting clones
 */
export async function deleteCloneAction(
  name: string,
  options: DeleteCloneActionOptions,
): Promise<void> {
  await loadConfig(options.config);
  const config = getConfig();
  const cloneService = new CloneService(config);

  try {
    await cloneService.deleteClone(name);
    log.success(`Clone '${name}' deleted successfully`);
  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    Deno.exit(1);
  }
}

/**
 * Common action for listing clones
 */
export async function listCloneAction(
  options: ListCloneActionOptions,
): Promise<void> {
  await loadConfig(options.config);
  const config = getConfig();
  const cloneService = new CloneService(config);

  try {
    const items = await cloneService.listClones();

    if (options.format === "json") {
      console.log(JSON.stringify(items, null, 2));
    } else {
      if (items.length === 0) {
        log.info("No clones found");
        return;
      }

      const table = new Table()
        .header(["Name", "Origin", "Used", "Created"])
        .body(
          items.map((item) => [
            item.name,
            item.origin,
            item.used,
            item.creation,
          ]),
        );

      console.log(table.toString());
    }
  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    Deno.exit(1);
  }
}

/**
 * Common action for showing clone info
 */
export async function infoCloneAction(
  name: string,
  options: InfoCloneActionOptions,
): Promise<void> {
  await loadConfig(options.config);
  const config = getConfig();
  const cloneService = new CloneService(config);

  try {
    const info = await cloneService.getCloneInfo(name);

    const table = new Table()
      .header(["Property", "Value"])
      .body([
        ["Clone Name", info.name],
        ["Port", info.port ? info.port.toString() : "N/A"],
        ["Origin", info.origin],
        ["Used", info.used],
        ["Created", info.creation],
      ]);

    console.log(table.toString());
  } catch (error) {
    log.error(error instanceof Error ? error.message : String(error));
    Deno.exit(1);
  }
}

/**
 * Common action for placeholder start/stop functionality
 */
export async function startStopCloneAction(
  name: string,
  action: "start" | "stop",
  options: { config?: string },
): Promise<void> {
  await loadConfig(options.config);

  log.info(
    `${
      action.charAt(0).toUpperCase() + action.slice(1)
    }ing clone '${name}' is not yet implemented`,
  );
  if (action === "start") {
    log.info(
      "Clone PostgreSQL instances are started automatically when created",
    );
  } else {
    log.info(
      "Clone PostgreSQL instances are stopped automatically when deleted",
    );
  }
}
