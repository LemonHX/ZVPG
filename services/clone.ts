import { log, runCommand, isPortInUse } from "../utils.ts";
import { Config } from "../config.ts";

export class CloneService {
  private config: Config;

  constructor(config: Config) {
    this.config = config;
  }

  /**
   * Create a clone from a snapshot
   * @param snapshotName - Name of the snapshot to clone from
   * @param cloneName - Name of the new clone
   * @param targetPort - Port for the cloned PostgreSQL instance
   * @returns Promise<void>
   */
  async createClone(snapshotName: string, cloneName: string, targetPort: number): Promise<void> {
    try {
      log.info(`Creating clone '${cloneName}' from snapshot '${snapshotName}'...`);

      // Check if port is already in use
      if (await isPortInUse(targetPort)) {
        throw new Error(`Port ${targetPort} is already in use`);
      }

      // Check if snapshot exists
      const snapshotExists = await this.checkSnapshotExists(snapshotName);
      if (!snapshotExists) {
        throw new Error(`Snapshot '${snapshotName}' does not exist`);
      }

      // Create clone from snapshot using ZFS clone command
      const cloneDataset = `${this.config.zfsPool}/${cloneName}`;
      const snapshotDataset = `${this.config.zfsPool}/${snapshotName}`;
      
      await runCommand(
        "zfs",
        ["clone", snapshotDataset, cloneDataset]
      );

      // Copy PostgreSQL configuration
      await this.copyPostgresConfig(snapshotName, cloneName);

      // Update port in PostgreSQL configuration
      await this.updatePostgresPort(cloneName, targetPort);

      // Start PostgreSQL instance for the clone
      await this.startPostgresInstance(cloneName, targetPort);

      log.success(`Clone '${cloneName}' created successfully on port ${targetPort}`);
    } catch (error) {
      const err = error as Error;
      log.error(`Failed to create clone: ${err.message}`);
      throw error;
    }
  }

  /**
   * Delete a clone
   * @param cloneName - Name of the clone to delete
   * @returns Promise<void>
   */
  async deleteClone(cloneName: string): Promise<void> {
    try {
      log.info(`Deleting clone '${cloneName}'...`);

      // Stop PostgreSQL instance for the clone
      await this.stopPostgresInstance(cloneName);

      // Destroy ZFS clone dataset
      const cloneDataset = `${this.config.zfsPool}/${cloneName}`;
      await runCommand("zfs", ["destroy", cloneDataset]);

      log.success(`Clone '${cloneName}' deleted successfully`);
    } catch (error) {
      const err = error as Error;
      log.error(`Failed to delete clone: ${err.message}`);
      throw error;
    }
  }

  /**
   * List all clones
   * @returns Promise<Array<{name: string, origin: string, used: string, creation: string}>>
   */
  async listClones(): Promise<Array<{name: string, origin: string, used: string, creation: string}>> {
    try {
      log.info("Listing all clones...");

      // Get list of clones (ZFS datasets that have an origin)
      const result = await runCommand(
        "zfs",
        ["list", "-r", "-t", "filesystem", "-o", "name,origin,used,creation", "-H", this.config.zfsPool]
      );

      const clones = [];
      const lines = result.stdout?.trim().split("\n") || [];
      
      for (const line of lines) {
        if (line.trim()) {
          const [fullName, origin, used, creation] = line.split("\t");
          
          // Skip if no origin (not a clone) or if it's the parent dataset
          if (!origin || origin === "-" || fullName === this.config.zfsPool) {
            continue;
          }

          // Extract clone name from full dataset path
          const name = fullName.replace(`${this.config.zfsPool}/`, "");
          
          clones.push({
            name,
            origin,
            used,
            creation
          });
        }
      }

      log.success(`Found ${clones.length} clones`);
      return clones;
    } catch (error) {
      const err = error as Error;
      log.error(`Failed to list clones: ${err.message}`);
      throw error;
    }
  }

  /**
   * Get information about a specific clone
   * @param cloneName - Name of the clone
   * @returns Promise<{name: string, origin: string, used: string, creation: string, port?: number}>
   */
  async getCloneInfo(cloneName: string): Promise<{name: string, origin: string, used: string, creation: string, port?: number}> {
    try {
      log.info(`Getting information for clone '${cloneName}'...`);

      const cloneDataset = `${this.config.zfsPool}/${cloneName}`;
      
      // Get clone properties
      const result = await runCommand(
        "zfs",
        ["list", "-o", "name,origin,used,creation", "-H", cloneDataset]
      );

      const [_fullName, origin, used, creation] = result.stdout?.trim().split("\t") || [];
      
      if (!origin || origin === "-") {
        throw new Error(`'${cloneName}' is not a clone`);
      }

      // Try to get the port from PostgreSQL configuration
      let port: number | undefined;
      try {
        port = await this.getPostgresPort(cloneName);
      } catch (error) {
        const err = error as Error;
        log.warn(`Could not determine PostgreSQL port for clone: ${err.message}`);
      }

      const cloneInfo = {
        name: cloneName,
        origin,
        used,
        creation,
        port
      };

      log.success(`Retrieved information for clone '${cloneName}'`);
      return cloneInfo;
    } catch (error) {
      const err = error as Error;
      log.error(`Failed to get clone info: ${err.message}`);
      throw error;
    }
  }

  /**
   * Check if a snapshot exists
   * @param snapshotName - Name of the snapshot to check
   * @returns Promise<boolean>
   */
  private async checkSnapshotExists(snapshotName: string): Promise<boolean> {
    try {
      const snapshotDataset = `${this.config.zfsPool}/${snapshotName}`;
      await runCommand("zfs", ["list", "-t", "snapshot", snapshotDataset]);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Copy PostgreSQL configuration from snapshot to clone
   * @param snapshotName - Source snapshot name
   * @param cloneName - Target clone name
   * @returns Promise<void>
   */
  private async copyPostgresConfig(snapshotName: string, cloneName: string): Promise<void> {
    const sourceConfigPath = `${this.config.mountDir}/${snapshotName}/postgresql.conf`;
    const targetConfigPath = `${this.config.mountDir}/${cloneName}/postgresql.conf`;

    await runCommand("cp", [sourceConfigPath, targetConfigPath]);
  }

  /**
   * Update PostgreSQL port in configuration
   * @param cloneName - Name of the clone
   * @param port - New port number
   * @returns Promise<void>
   */
  private async updatePostgresPort(cloneName: string, port: number): Promise<void> {
    const configPath = `${this.config.mountDir}/${cloneName}/postgresql.conf`;
    
    await runCommand("sed", ["-i", `s/^#*port = .*/port = ${port}/`, configPath]);
  }

  /**
   * Start PostgreSQL instance for a clone
   * @param cloneName - Name of the clone
   * @param port - Port number
   * @returns Promise<void>
   */
  private async startPostgresInstance(cloneName: string, port: number): Promise<void> {
    const dataDir = `${this.config.mountDir}/${cloneName}`;
    const logFile = `${dataDir}/postgresql.log`;

    await runCommand("sudo", [
      "-u", "postgres", 
      "pg_ctl", "start", 
      "-D", dataDir, 
      "-l", logFile, 
      "-o", `-p ${port}`
    ]);

    // Wait a moment for PostgreSQL to start
    await new Promise(resolve => setTimeout(resolve, 2000));
  }

  /**
   * Stop PostgreSQL instance for a clone
   * @param cloneName - Name of the clone
   * @returns Promise<void>
   */
  private async stopPostgresInstance(cloneName: string): Promise<void> {
    const dataDir = `${this.config.mountDir}/${cloneName}`;

    try {
      await runCommand("sudo", [
        "-u", "postgres", 
        "pg_ctl", "stop", 
        "-D", dataDir, 
        "-m", "fast"
      ]);
    } catch (error) {
      const err = error as Error;
      log.warn(`Failed to stop PostgreSQL instance gracefully: ${err.message}`);
      
      // Try force stop
      try {
        await runCommand("sudo", [
          "-u", "postgres", 
          "pg_ctl", "stop", 
          "-D", dataDir, 
          "-m", "immediate"
        ]);
      } catch (forceError) {
        const forceErr = forceError as Error;
        log.warn(`Failed to force stop PostgreSQL instance: ${forceErr.message}`);
      }
    }
  }

  /**
   * Get PostgreSQL port from configuration
   * @param cloneName - Name of the clone
   * @returns Promise<number>
   */
  private async getPostgresPort(cloneName: string): Promise<number> {
    const configPath = `${this.config.mountDir}/${cloneName}/postgresql.conf`;
    
    const result = await runCommand("grep", ["^port = ", configPath]);
    const portLine = result.stdout?.trim();
    
    if (!portLine) {
      throw new Error("Port line not found in PostgreSQL configuration");
    }

    const port = parseInt(portLine.split(" ")[2]);
    if (isNaN(port)) {
      throw new Error("Could not parse port from PostgreSQL configuration");
    }

    return port;
  }
}
