import { type Config } from "../config.ts";
import { isPortInUse, log, runCommand } from "../utils.ts";

export interface SystemStatus {
  zfsPool: {
    name: string;
    health: string;
    size: string;
    used: string;
    available: string;
  };
  postgres: {
    version: string;
    running: boolean;
    mainPort: number;
  };
  clones: {
    total: number;
    active: number;
    inactive: number;
    details: Array<{
      name: string;
      port: number;
      status: "running" | "stopped" | "unknown";
      size: string;
      created: string;
    }>;
  };
  snapshots: {
    total: number;
    totalSize: string;
    details: Array<{
      name: string;
      size: string;
      created: string;
      referenced: string;
    }>;
  };
  system: {
    hostname: string;
    uptime: string;
    loadAverage: string;
    memoryUsage: string;
    diskUsage: string;
  };
}

export class StatusService {
  private config: Config;

  constructor(config: Config) {
    this.config = config;
  }

  /**
   * Get comprehensive system status
   * @returns Promise<SystemStatus>
   */
  async getSystemStatus(): Promise<SystemStatus> {
    log.info("Gathering system status...");
    try {
      const [
        zfsPool,
        postgres,
        snapshots,
        clones,
        system,
      ] = await Promise.all([
        this.getZfsPoolStatus(),
        this.getPostgresStatus(),
        this.getSnapshotsStatus(),
        this.getClonesStatus(),
        this.getSystemInfo(),
      ]);

      const status: SystemStatus = {
        zfsPool,
        postgres,
        snapshots,
        clones,
        system,
      };

      log.success("Successfully gathered system status.");
      return status;
    } catch (error) {
      const err = error as Error;
      log.error(`Failed to get system status: ${err.message}`);
      throw err;
    }
  }

  /**
   * Get ZFS pool status
   * @returns Promise<SystemStatus['zfsPool']>
   */
  private async getZfsPoolStatus(): Promise<SystemStatus["zfsPool"]> {
    try {
      const result = await runCommand("zfs", [
        "list",
        "-o",
        "name,health,size,used,available",
        "-H",
        this.config.zfsPool,
      ]);
      if (!result.success || !result.stdout) {
        throw new Error(result.stderr || "Failed to get ZFS pool status");
      }
      const [name, health, size, used, available] = result.stdout.trim().split(
        "\t",
      );
      return { name, health, size, used, available };
    } catch (error) {
      const err = error as Error;
      log.warn(`Could not get ZFS pool status: ${err.message}`);
      return {
        name: this.config.zfsPool,
        health: "UNKNOWN",
        size: "N/A",
        used: "N/A",
        available: "N/A",
      };
    }
  }

  /**
   * Get PostgreSQL status
   * @returns Promise<SystemStatus['postgres']>
   */
  private async getPostgresStatus(): Promise<SystemStatus["postgres"]> {
    try {
      const versionResult = await runCommand("pg_config", ["--version"]);
      const version = versionResult.stdout?.trim() ?? "Unknown";

      const statusResult = await runCommand("pg_ctl", [
        "status",
        "-D",
        `${this.config.mountDir}/${this.config.dataSubdir}`,
      ]);
      const running = statusResult.success;

      // The main port is not explicitly in the config, let's assume it's the start of the clone range - 1 or a default
      const mainPort = this.config.clonePortStart - 1;

      return {
        version,
        running,
        mainPort,
      };
    } catch (error) {
      const err = error as Error;
      log.warn(`Could not get PostgreSQL status: ${err.message}`);
      return {
        version: "Unknown",
        running: false,
        mainPort: this.config.clonePortStart - 1,
      };
    }
  }

  /**
   * Get snapshots status
   * @returns Promise<SystemStatus['snapshots']>
   */
  private async getSnapshotsStatus(): Promise<SystemStatus["snapshots"]> {
    try {
      const result = await runCommand("zfs", [
        "list",
        "-t",
        "snapshot",
        "-r",
        this.config.zfsPool,
        "-o",
        "name,used,refer,creation",
        "-H",
        "-s",
        "creation",
      ]);

      if (!result.success || !result.stdout) {
        throw new Error(result.stderr || "Failed to list snapshots");
      }

      const details = result.stdout.trim().split("\n").filter(Boolean).map(
        (line) => {
          const [fullName, size, referenced, created] = line.split("\t");
          const name = fullName.split("@")[1];
          return { name, size, referenced, created };
        },
      );

      const totalSizeResult = await runCommand("zfs", [
        "list",
        "-t",
        "snapshot",
        "-r",
        this.config.zfsPool,
        "-o",
        "used",
        "-H",
        "-p",
      ]);

      if (!totalSizeResult.success || !totalSizeResult.stdout) {
        throw new Error(
          totalSizeResult.stderr || "Failed to calculate total snapshot size",
        );
      }

      const totalSize = totalSizeResult.stdout.trim().split("\n").reduce(
        (acc, size) => acc + Number(size),
        0,
      );

      return {
        total: details.length,
        totalSize: this.formatBytes(totalSize),
        details,
      };
    } catch (error) {
      const err = error as Error;
      log.warn(`Could not get snapshots status: ${err.message}`);
      return { total: 0, totalSize: "N/A", details: [] };
    }
  }

  /**
   * Get clones status
   * @returns Promise<SystemStatus['clones']>
   */
  private async getClonesStatus(): Promise<SystemStatus["clones"]> {
    try {
      const result = await runCommand("zfs", [
        "list",
        "-r",
        "-t",
        "filesystem",
        "-o",
        "name,origin,used,creation",
        "-H",
        this.config.zfsPool,
      ]);

      if (!result.success || !result.stdout) {
        throw new Error(result.stderr || "Failed to list clones");
      }

      const lines = result.stdout.trim().split("\n").filter(Boolean);
      const details: SystemStatus["clones"]["details"] = [];
      let active = 0;

      for (const line of lines) {
        const [fullName, origin, size, created] = line.split("\t");
        if (!origin || origin === "-") continue;

        const name = fullName.substring(fullName.lastIndexOf("/") + 1);
        const port = await this.getClonePort(name);
        const status = await this.getCloneStatus(port);

        if (status === "running") {
          active++;
        }

        details.push({ name, port, status, size, created });
      }

      return {
        total: details.length,
        active,
        inactive: details.length - active,
        details,
      };
    } catch (error) {
      const err = error as Error;
      log.warn(`Could not get clones status: ${err.message}`);
      return { total: 0, active: 0, inactive: 0, details: [] };
    }
  }

  /**
   * Get system information
   * @returns Promise<SystemStatus['system']>
   */
  private async getSystemInfo(): Promise<SystemStatus["system"]> {
    try {
      const [hostnameRes, uptimeRes, freeRes, dfRes, loadAvgRes] = await Promise
        .all([
          runCommand("hostname"),
          runCommand("uptime", ["-p"]),
          runCommand("free", ["-h"]),
          runCommand("df", ["-h", "/"]),
          runCommand("uptime"),
        ]);

      const uptime = uptimeRes.stdout?.trim().replace("up ", "") ?? "N/A";
      const loadAverage =
        loadAvgRes.stdout?.split("load average:")[1]?.trim() ?? "N/A";
      const memoryLine = freeRes.stdout?.split("\n")[1] ?? "";
      const memoryUsage = `${memoryLine.split(/\s+/)[2]} / ${
        memoryLine.split(/\s+/)[1]
      }`;
      const diskLine = dfRes.stdout?.split("\n")[1] ?? "";
      const diskUsage = `${diskLine.split(/\s+/)[2]} / ${
        diskLine.split(/\s+/)[1]
      } (${diskLine.split(/\s+/)[4]})`;

      return {
        hostname: hostnameRes.stdout?.trim() ?? "N/A",
        uptime,
        loadAverage,
        memoryUsage,
        diskUsage,
      };
    } catch (error) {
      const err = error as Error;
      log.warn(`Could not get system info: ${err.message}`);
      return {
        hostname: "N/A",
        uptime: "N/A",
        loadAverage: "N/A",
        memoryUsage: "N/A",
        diskUsage: "N/A",
      };
    }
  }

  /**
   * Get clone port
   * @param cloneName - Name of the clone
   * @returns Promise<number>
   */
  private async getClonePort(cloneName: string): Promise<number> {
    try {
      const configPath =
        `${this.config.mountDir}/${this.config.clonesSubdir}/${cloneName}/postgresql.conf`;
      const result = await runCommand("grep", ["^port = ", configPath]);
      if (!result.success || !result.stdout) {
        return 0;
      }
      const portLine = result.stdout.trim();
      return portLine ? parseInt(portLine.split("=")[1].trim(), 10) : 0;
    } catch {
      return 0; // Not found or other error
    }
  }

  /**
   * Get clone status (running/stopped/unknown)
   * @param port - Port of the clone
   * @returns Promise<"running" | "stopped" | "unknown">
   */
  private async getCloneStatus(
    port: number,
  ): Promise<"running" | "stopped" | "unknown"> {
    if (port === 0) return "unknown";
    try {
      return await isPortInUse(port) ? "running" : "stopped";
    } catch (error) {
      const err = error as Error;
      log.warn(`Could not check port status for ${port}: ${err.message}`);
      return "unknown";
    }
  }

  /**
   * Format bytes to human readable format
   * @param bytes - Number of bytes
   * @returns Formatted string
   */
  private formatBytes(bytes: number): string {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
  }
}
