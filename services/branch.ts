// ZFS Verioned PostgreSQL Engine - Branch Service

import {
  formatISOTimestamp,
  isPortInUse,
  log,
  runCommand,
  validateBranchName,
} from "../utils.ts";
import { getConfig } from "../config.ts";

export interface BranchInfo {
  name: string;
  parentBranch: string;
  parentSnapshot: string;
  created: string;
  dataset: string;
  mount: string;
  used: string;
  available: string;
  referenced: string;
  compressratio: string;
  clones: string[];
  port?: number;
  pgStatus?: "running" | "stopped" | "unknown";
}

export class BranchService {
  private config = getConfig();

  /**
   * 创建新分支
   * @param name 分支名称
   * @param port 端口号，用于启动PostgreSQL实例
   * @param parentSnapshot 父快照名称
   * @param parentBranch 父分支名称
   */
  async createBranch(
    name: string,
    port: number,
    parentSnapshot?: string,
    parentBranch = "main",
  ): Promise<void> {
    if (!validateBranchName(name)) {
      throw new Error("Invalid branch name format");
    }

    const branchDataset = this.getBranchDataset(name);

    // 检查分支是否已存在
    log.debug(`Checking if branch already exists: ${branchDataset}`);
    const branchExists = await runCommand("zfs", ["list", branchDataset], {
      stdout: "null",
      stderr: "null",
    });

    if (branchExists.success) {
      throw new Error(`Branch already exists: ${name}`);
    }

    // 获取父快照
    if (!parentSnapshot) {
      log.debug("No parent snapshot specified, getting latest snapshot");
      const latestSnapshot = await this.getLatestSnapshot();
      if (!latestSnapshot) {
        throw new Error(
          "No snapshots available. Please create a snapshot first.",
        );
      }
      parentSnapshot = latestSnapshot;
    }

    // 格式化快照名称
    if (!parentSnapshot.includes("@")) {
      parentSnapshot =
        `${this.config.zfsPool}/${this.config.dataSubdir}@${parentSnapshot}`;
    }

    // 验证父快照是否存在
    log.debug(`Verifying parent snapshot exists: ${parentSnapshot}`);
    if (!(await this.snapshotExists(parentSnapshot))) {
      throw new Error(`Parent snapshot does not exist: ${parentSnapshot}`);
    }

    // 创建branches目录（如果不存在）
    const branchesDir = `${this.config.zfsPool}/branches`;
    log.debug(`Checking if branches directory exists: ${branchesDir}`);
    const branchesDirExists = await runCommand("zfs", ["list", branchesDir], {
      stdout: "null",
      stderr: "null",
    });

    if (!branchesDirExists.success) {
      log.info(`Creating branches directory: ${branchesDir}`);
      // 创建branches目录
      const createResult = await runCommand("zfs", ["create", branchesDir]);
      if (!createResult.success) {
        throw new Error(
          `Failed to create branches directory: ${createResult.stderr}`,
        );
      }
    }

    log.info(`Creating branch: ${name} from ${parentSnapshot}`);

    // 创建分支克隆
    const cloneResult = await runCommand("zfs", [
      "clone",
      parentSnapshot,
      branchDataset,
    ]);

    if (!cloneResult.success) {
      throw new Error(`Failed to create branch: ${cloneResult.stderr}`);
    }

    // 设置分支属性
    log.debug(`Setting branch properties for: ${branchDataset}`);
    await runCommand("zfs", [
      "set",
      `zvpg:branch_name=${name}`,
      branchDataset,
    ]);
    await runCommand("zfs", [
      "set",
      `zvpg:parent_branch=${parentBranch}`,
      branchDataset,
    ]);
    await runCommand("zfs", [
      "set",
      `zvpg:parent_snapshot=${parentSnapshot}`,
      branchDataset,
    ]);
    await runCommand("zfs", [
      "set",
      `zvpg:created=${formatISOTimestamp()}`,
      branchDataset,
    ]);

    const branchMount = this.getBranchMount(name);
    log.success(`Branch created successfully: ${name}`);
    log.info(`Branch mount: ${branchMount}`);

    // Always start PostgreSQL instance for new branch
    await this.startBranchPostgres(name, port);
  }

  /**
   * 删除分支
   * @param name 分支名称
   * @param force 是否强制删除
   */
  async deleteBranch(name: string, force = false): Promise<void> {
    const branchDataset = this.getBranchDataset(name);

    // 检查分支是否存在
    log.debug(`Checking if branch exists: ${branchDataset}`);
    const branchExists = await runCommand("zfs", ["list", branchDataset], {
      stdout: "null",
      stderr: "null",
    });

    if (!branchExists.success) {
      throw new Error(`Branch does not exist: ${name}`);
    }

    // 检查是否有依赖的克隆
    log.debug(`Checking for dependent clones in branch: ${branchDataset}`);
    const clonesResult = await runCommand("zfs", [
      "list",
      "-t",
      "filesystem",
      "-r",
      branchDataset,
      "-H",
      "-o",
      "name",
    ]);

    if (clonesResult.success && clonesResult.stdout) {
      const clones = clonesResult.stdout.trim().split("\n").filter(Boolean)
        .filter((clone: string) => clone !== branchDataset);

      if (clones.length > 0 && !force) {
        const cloneList = clones.join(", ");
        throw new Error(
          `Cannot delete branch ${name} - it has dependent clones: ${cloneList}. Use --force to delete anyway.`,
        );
      }
    }

    // Stop PostgreSQL instance if running
    try {
      await this.stopBranchPostgres(name);
    } catch (error) {
      log.warn(`Failed to stop PostgreSQL for branch '${name}': ${error}`);
    }

    log.info(`Deleting branch: ${name}`);

    // 删除分支
    const deleteResult = await runCommand("zfs", ["destroy", branchDataset]);

    if (!deleteResult.success) {
      throw new Error(`Failed to delete branch: ${deleteResult.stderr}`);
    }

    log.success(`Branch deleted successfully: ${name}`);
  }

  /**
   * 列出所有分支
   */
  async listBranches(): Promise<BranchInfo[]> {
    log.debug(`Listing branches for pool: ${this.config.zfsPool}`);

    // 获取所有分支数据集
    const listResult = await runCommand("zfs", [
      "list",
      "-t",
      "filesystem",
      "-r",
      `${this.config.zfsPool}/branches`,
      "-H",
      "-o",
      "name",
    ]);

    if (!listResult.success || !listResult.stdout) {
      return [];
    }

    const branches = listResult.stdout.trim().split("\n").filter(Boolean)
      .filter((branch: string) => branch !== `${this.config.zfsPool}/branches`);

    const branchInfos: BranchInfo[] = [];

    for (const branch of branches) {
      const branchName = branch.split("/").pop() || "";
      const branchInfo = await this.getBranchInfo(branchName);
      branchInfos.push(branchInfo);
    }

    return branchInfos;
  }

  /**
   * 获取分支详细信息
   * @param name 分支名称
   */
  async getBranchInfo(name: string): Promise<BranchInfo> {
    const branchDataset = this.getBranchDataset(name);

    // 检查分支是否存在
    log.debug(`Getting info for branch: ${branchDataset}`);
    const branchExists = await runCommand("zfs", ["list", branchDataset], {
      stdout: "null",
      stderr: "null",
    });

    if (!branchExists.success) {
      throw new Error(`Branch does not exist: ${name}`);
    }

    // 获取ZFS统计信息
    const statsResult = await runCommand("zfs", [
      "list",
      "-H",
      "-o",
      "name,used,available,referenced,compressratio",
      branchDataset,
    ]);

    let used = "", available = "", referenced = "", compressratio = "";
    if (statsResult.success && statsResult.stdout) {
      [, used, available, referenced, compressratio] = statsResult.stdout.trim()
        .split("\t");
    }

    // 获取元数据
    const parentBranch = await this.getBranchProperty(
      branchDataset,
      "zvpg:parent_branch",
    );
    const parentSnapshot = await this.getBranchProperty(
      branchDataset,
      "zvpg:parent_snapshot",
    );
    const created = await this.getBranchProperty(
      branchDataset,
      "zvpg:created",
    );

    // 获取克隆列表
    const clones = await this.getBranchClones(branchDataset);

    // 获取PostgreSQL端口和状态
    const port = await this.getBranchPostgresPort(name);
    const pgStatus = port
      ? await this.getBranchPostgresStatus(name)
      : undefined;

    return {
      name,
      parentBranch,
      parentSnapshot,
      created,
      dataset: branchDataset,
      mount: this.getBranchMount(name),
      used,
      available,
      referenced,
      compressratio,
      clones,
      port,
      pgStatus,
    };
  }

  /**
   * 从分支创建快照
   * @param branchName 分支名称
   * @param snapshotName 快照名称
   * @param message 快照消息
   */
  async createBranchSnapshot(
    branchName: string,
    snapshotName: string,
    message = "Branch snapshot",
  ): Promise<void> {
    const branchDataset = this.getBranchDataset(branchName);

    // 检查分支是否存在
    log.debug(`Checking if branch exists: ${branchDataset}`);
    const branchExists = await runCommand("zfs", ["list", branchDataset], {
      stdout: "null",
      stderr: "null",
    });

    if (!branchExists.success) {
      throw new Error(`Branch does not exist: ${branchName}`);
    }

    const fullSnapshotName = `${branchDataset}@${snapshotName}`;

    // 检查快照是否已存在
    log.debug(`Checking if snapshot already exists: ${fullSnapshotName}`);
    if (await this.snapshotExists(fullSnapshotName)) {
      throw new Error(`Snapshot already exists: ${fullSnapshotName}`);
    }

    log.info(`Creating snapshot from branch: ${branchName}`);

    // 创建快照
    const createResult = await runCommand("zfs", [
      "snapshot",
      fullSnapshotName,
    ]);

    if (!createResult.success) {
      throw new Error(
        `Failed to create branch snapshot: ${createResult.stderr}`,
      );
    }

    // 添加元数据
    log.debug(`Setting snapshot properties for: ${fullSnapshotName}`);
    await runCommand("zfs", [
      "set",
      `zvpg:message=${message}`,
      fullSnapshotName,
    ]);
    await runCommand("zfs", [
      "set",
      `zvpg:branch=${branchName}`,
      fullSnapshotName,
    ]);
    await runCommand("zfs", [
      "set",
      `zvpg:created=${formatISOTimestamp()}`,
      fullSnapshotName,
    ]);

    log.success(`Branch snapshot created successfully: ${fullSnapshotName}`);
  }

  /**
   * 启动分支的PostgreSQL实例
   * @param name 分支名称
   * @param port 端口号
   */
  async startBranchPostgres(name: string, port: number): Promise<void> {
    if (await isPortInUse(port)) {
      throw new Error(`Port ${port} is already in use`);
    }

    const branchDataset = this.getBranchDataset(name);

    // Check if branch exists
    const branchExists = await runCommand("zfs", ["list", branchDataset], {
      stdout: "null",
      stderr: "null",
    });

    if (!branchExists.success) {
      throw new Error(`Branch does not exist: ${name}`);
    }

    // Copy/update PostgreSQL configuration
    await this.updatePostgresConfig(name, port);

    // Start PostgreSQL instance
    await this.startPostgresInstance(name, port);

    // Store port in branch properties
    await runCommand("zfs", [
      "set",
      `zvpg:port=${port}`,
      branchDataset,
    ]);

    log.success(
      `PostgreSQL instance started for branch '${name}' on port ${port}`,
    );
  }

  /**
   * 停止分支的PostgreSQL实例
   * @param name 分支名称
   */
  async stopBranchPostgres(name: string): Promise<void> {
    const branchDataset = this.getBranchDataset(name);

    // Check if branch exists
    const branchExists = await runCommand("zfs", ["list", branchDataset], {
      stdout: "null",
      stderr: "null",
    });

    if (!branchExists.success) {
      throw new Error(`Branch does not exist: ${name}`);
    }

    // Stop PostgreSQL instance
    await this.stopPostgresInstance(name);

    // Remove port from branch properties
    await runCommand("zfs", [
      "inherit",
      "zvpg:port",
      branchDataset,
    ]);

    log.success(`PostgreSQL instance stopped for branch '${name}'`);
  }

  // 私有辅助方法

  private getBranchDataset(branchName: string): string {
    return `${this.config.zfsPool}/branches/${branchName}`;
  }

  private getBranchMount(branchName: string): string {
    return `${this.config.mountDir}/${this.config.zfsPool}/branches/${branchName}`;
  }

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

  private async getLatestSnapshot(): Promise<string | null> {
    log.debug(
      `Getting latest snapshot from: ${this.config.zfsPool}/${this.config.dataSubdir}`,
    );
    const result = await runCommand("zfs", [
      "list",
      "-t",
      "snapshot",
      "-r",
      `${this.config.zfsPool}/${this.config.dataSubdir}`,
      "-H",
      "-o",
      "name",
      "-s",
      "creation",
    ]);

    if (!result.success || !result.stdout) {
      return null;
    }

    const snapshots = result.stdout.trim().split("\n").filter(Boolean);
    return snapshots.length > 0 ? snapshots[snapshots.length - 1] : null;
  }

  private async getBranchProperty(
    dataset: string,
    property: string,
  ): Promise<string> {
    log.debug(`Getting property ${property} from dataset: ${dataset}`);
    const result = await runCommand("zfs", [
      "get",
      "-H",
      "-o",
      "value",
      property,
      dataset,
    ]);
    return result.success ? (result.stdout?.trim() || "") : "";
  }

  private async getBranchClones(branchDataset: string): Promise<string[]> {
    log.debug(`Getting clones for branch: ${branchDataset}`);
    const result = await runCommand("zfs", [
      "list",
      "-t",
      "filesystem",
      "-r",
      branchDataset,
      "-H",
      "-o",
      "name",
    ]);

    const clones: string[] = [];
    if (result.success && result.stdout) {
      result.stdout.trim().split("\n").forEach((line: string) => {
        if (line !== branchDataset) {
          clones.push(line.replace(`${branchDataset}/`, ""));
        }
      });
    }

    return clones;
  }

  /**
   * Update PostgreSQL configuration for a branch
   * @param branchName - Name of the branch
   * @param port - Port number
   */
  private async updatePostgresConfig(
    branchName: string,
    port: number,
  ): Promise<void> {
    const branchMount = this.getBranchMount(branchName);
    const configPath = `${branchMount}/postgresql.conf`;

    // Update port in PostgreSQL configuration
    await runCommand("sed", [
      "-i",
      `s/^#*port = .*/port = ${port}/`,
      configPath,
    ]);

    // Update socket directory
    const socketDir = `${this.config.mountDir}/${this.config.socketSubdir}`;
    await runCommand("sed", [
      "-i",
      `s|^#*unix_socket_directories = .*|unix_socket_directories = '${socketDir}'|`,
      configPath,
    ]);
  }

  /**
   * Start PostgreSQL instance for a branch
   * @param branchName - Name of the branch
   * @param port - Port number
   */
  private async startPostgresInstance(
    branchName: string,
    port: number,
  ): Promise<void> {
    const branchMount = this.getBranchMount(branchName);
    const logFile = `${branchMount}/postgresql.log`;

    await runCommand("sudo", [
      "-u",
      this.config.postgresUser,
      "pg_ctl",
      "start",
      "-D",
      branchMount,
      "-l",
      logFile,
      "-o",
      `-p ${port}`,
    ]);

    // Wait a moment for PostgreSQL to start
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }

  /**
   * Stop PostgreSQL instance for a branch
   * @param branchName - Name of the branch
   */
  private async stopPostgresInstance(branchName: string): Promise<void> {
    const branchMount = this.getBranchMount(branchName);

    try {
      await runCommand("sudo", [
        "-u",
        this.config.postgresUser,
        "pg_ctl",
        "stop",
        "-D",
        branchMount,
        "-m",
        "fast",
      ]);
    } catch (error) {
      const err = error as Error;
      log.warn(`Failed to stop PostgreSQL instance gracefully: ${err.message}`);

      // Try force stop
      try {
        await runCommand("sudo", [
          "-u",
          this.config.postgresUser,
          "pg_ctl",
          "stop",
          "-D",
          branchMount,
          "-m",
          "immediate",
        ]);
      } catch (forceError) {
        const forceErr = forceError as Error;
        log.warn(
          `Failed to force stop PostgreSQL instance: ${forceErr.message}`,
        );
      }
    }
  }

  /**
   * Get PostgreSQL port from branch properties
   * @param branchName - Name of the branch
   * @returns Port number or undefined if not set
   */
  private async getBranchPostgresPort(
    branchName: string,
  ): Promise<number | undefined> {
    const branchDataset = this.getBranchDataset(branchName);
    const portStr = await this.getBranchProperty(branchDataset, "zvpg:port");

    if (!portStr || portStr === "-") {
      return undefined;
    }

    const port = parseInt(portStr);
    return isNaN(port) ? undefined : port;
  }

  /**
   * Check PostgreSQL instance status for a branch
   * @param branchName - Name of the branch
   * @returns Status string
   */
  private async getBranchPostgresStatus(
    branchName: string,
  ): Promise<"running" | "stopped" | "unknown"> {
    const branchMount = this.getBranchMount(branchName);

    try {
      const result = await runCommand("sudo", [
        "-u",
        this.config.postgresUser,
        "pg_ctl",
        "status",
        "-D",
        branchMount,
      ]);

      if (result.success && result.stdout?.includes("server is running")) {
        return "running";
      } else {
        return "stopped";
      }
    } catch {
      return "unknown";
    }
  }
}
