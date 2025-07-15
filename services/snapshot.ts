// ZFS Verioned PostgreSQL Engine - Snapshot Service

import {
  formatISOTimestamp,
  log,
  runCommand,
  validateSnapshotName,
} from "../utils.ts";
import { getConfig } from "../config.ts";

export interface SnapshotInfo {
  name: string;
  fullName: string;
  used: string;
  available: string;
  referenced: string;
  compressratio: string;
  creation: string;
  message: string;
  created: string;
  clones: string[];
}

export class SnapshotService {
  private config = getConfig();

  /**
   * 创建快照
   * @param name 快照名称
   * @param message 快照描述信息
   */
  async createSnapshot(
    name: string,
    message = "Manual snapshot",
  ): Promise<void> {
    if (!validateSnapshotName(name)) {
      throw new Error("Invalid snapshot name format");
    }

    const fullSnapshotName =
      `${this.config.zfsPool}/${this.config.dataSubdir}@${name}`;

    // 检查快照是否已存在
    log.debug(`Checking if snapshot already exists: ${fullSnapshotName}`);
    if (await this.snapshotExists(fullSnapshotName)) {
      throw new Error(`Snapshot already exists: ${fullSnapshotName}`);
    }

    // 检查数据目录是否存在
    log.debug(
      `Checking if data directory exists: ${this.config.zfsPool}/${this.config.dataSubdir}`,
    );
    const dataExists = await runCommand("zfs", [
      "list",
      `${this.config.zfsPool}/${this.config.dataSubdir}`,
    ], {
      stdout: "null",
      stderr: "null",
    });

    if (!dataExists.success) {
      throw new Error(
        `Data directory does not exist: ${this.config.zfsPool}/${this.config.dataSubdir}`,
      );
    }

    log.info(`Creating snapshot: ${fullSnapshotName}`);

    // 创建ZFS快照
    const createResult = await runCommand("zfs", [
      "snapshot",
      fullSnapshotName,
    ]);

    if (!createResult.success) {
      throw new Error(`Failed to create snapshot: ${createResult.stderr}`);
    }

    // 添加快照元数据
    log.debug(`Setting snapshot metadata for: ${fullSnapshotName}`);
    await runCommand("zfs", [
      "set",
      `zvpg:message=${message}`,
      fullSnapshotName,
    ]);
    await runCommand("zfs", [
      "set",
      `zvpg:created=${formatISOTimestamp()}`,
      fullSnapshotName,
    ]);

    log.success(`Snapshot created successfully: ${fullSnapshotName}`);
  }

  /**
   * 删除快照
   * @param name 快照名称
   * @param force 是否强制删除
   */
  async deleteSnapshot(name: string, force = false): Promise<void> {
    let fullSnapshotName = name;

    if (!name.includes("@")) {
      fullSnapshotName =
        `${this.config.zfsPool}/${this.config.dataSubdir}@${name}`;
    }

    // 检查快照是否存在
    log.debug(`Checking if snapshot exists: ${fullSnapshotName}`);
    if (!(await this.snapshotExists(fullSnapshotName))) {
      throw new Error(`Snapshot does not exist: ${fullSnapshotName}`);
    }

    // 检查是否有依赖的克隆
    log.debug(`Checking for dependent clones of snapshot: ${fullSnapshotName}`);
    const clonesResult = await runCommand("zfs", [
      "list",
      "-t",
      "filesystem",
      "-r",
      this.config.zfsPool,
      "-H",
      "-o",
      "name,origin",
    ]);

    if (clonesResult.success && clonesResult.stdout) {
      const clones = clonesResult.stdout.trim().split("\n")
        .filter((line: string) => line.includes(fullSnapshotName))
        .map((line: string) => line.split("\t")[0]);

      if (clones.length > 0 && !force) {
        const cloneList = clones.join(", ");
        throw new Error(
          `Cannot delete snapshot ${fullSnapshotName} - it has dependent clones: ${cloneList}. Use --force to delete anyway.`,
        );
      }
    }

    log.info(`Deleting snapshot: ${fullSnapshotName}`);

    // 删除ZFS快照
    const deleteResult = await runCommand("zfs", ["destroy", fullSnapshotName]);

    if (!deleteResult.success) {
      throw new Error(`Failed to delete snapshot: ${deleteResult.stderr}`);
    }

    log.success(`Snapshot deleted successfully: ${fullSnapshotName}`);
  }

  /**
   * 列出所有快照
   */
  async listSnapshots(): Promise<SnapshotInfo[]> {
    log.debug(`Listing snapshots for pool: ${this.config.zfsPool}`);

    // 获取所有快照及其基本信息
    const listResult = await runCommand("zfs", [
      "list",
      "-t",
      "snapshot",
      "-r",
      this.config.zfsPool,
      "-H",
      "-o",
      "name,used,referenced,creation",
    ]);

    if (!listResult.success || !listResult.stdout) {
      return [];
    }

    const snapshots = listResult.stdout.trim().split("\n").filter(Boolean);
    const snapshotInfos: SnapshotInfo[] = [];

    for (const snapshot of snapshots) {
      const [name, _used, _referenced, _creation] = snapshot.split("\t");
      const snapshotName = name.split("@")[1] || name;

      try {
        const snapshotInfo = await this.getSnapshotInfo(snapshotName);
        snapshotInfos.push(snapshotInfo);
      } catch (error) {
        log.warn(
          `Failed to get info for snapshot ${snapshotName}: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }

    return snapshotInfos;
  }

  /**
   * 获取快照详细信息
   * @param name 快照名称
   */
  async getSnapshotInfo(name: string): Promise<SnapshotInfo> {
    let fullSnapshotName = name;

    if (!name.includes("@")) {
      fullSnapshotName =
        `${this.config.zfsPool}/${this.config.dataSubdir}@${name}`;
    }

    // 检查快照是否存在
    log.debug(`Getting info for snapshot: ${fullSnapshotName}`);
    if (!(await this.snapshotExists(fullSnapshotName))) {
      throw new Error(`Snapshot does not exist: ${fullSnapshotName}`);
    }

    // 获取快照统计信息
    const statsResult = await runCommand("zfs", [
      "list",
      "-t",
      "snapshot",
      "-H",
      "-o",
      "name,used,available,referenced,compressratio,creation",
      fullSnapshotName,
    ]);

    let used = "",
      available = "",
      referenced = "",
      compressratio = "",
      creation = "";
    if (statsResult.success && statsResult.stdout) {
      [, used, available, referenced, compressratio, creation] = statsResult
        .stdout.trim().split("\t");
    }

    // 获取元数据
    const message = await this.getSnapshotProperty(
      fullSnapshotName,
      "zvpg:message",
    );
    const created = await this.getSnapshotProperty(
      fullSnapshotName,
      "zvpg:created",
    );

    // 获取克隆列表
    const clones = await this.getSnapshotClones(fullSnapshotName);

    return {
      name: name.split("@")[1] || name,
      fullName: fullSnapshotName,
      used,
      available,
      referenced,
      compressratio,
      creation,
      message,
      created,
      clones,
    };
  }

  // 私有辅助方法

  private async snapshotExists(snapshotName: string): Promise<boolean> {
    const result = await runCommand("zfs", [
      "list",
      "-t",
      "snapshot",
      snapshotName,
    ], {
      stdout: "null",
      stderr: "null",
    });
    return result.success;
  }

  private async getSnapshotProperty(
    snapshot: string,
    property: string,
  ): Promise<string> {
    log.debug(`Getting property ${property} from snapshot: ${snapshot}`);
    const result = await runCommand("zfs", [
      "get",
      "-H",
      "-o",
      "value",
      property,
      snapshot,
    ]);
    return result.success ? (result.stdout?.trim() || "") : "";
  }

  private async getSnapshotClones(fullSnapshotName: string): Promise<string[]> {
    log.debug(`Getting clones for snapshot: ${fullSnapshotName}`);
    const result = await runCommand("zfs", [
      "list",
      "-t",
      "filesystem",
      "-r",
      this.config.zfsPool,
      "-H",
      "-o",
      "name,origin",
    ]);

    const clones: string[] = [];
    if (result.success && result.stdout) {
      result.stdout.trim().split("\n").forEach((line: string) => {
        const [cloneName, origin] = line.split("\t");
        if (origin === fullSnapshotName) {
          clones.push(cloneName);
        }
      });
    }

    return clones;
  }
}
