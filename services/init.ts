import { log, runCommand } from "../utils.ts";
import { Config } from "../config.ts";

export class InitService {
  private config: Config;

  constructor(config: Config) {
    this.config = config;
  }

  /**
   * Initialize the ZFS Verioned PostgreSQL Engine environment
   * @returns Promise<void>
   */
  async initializeEnvironment(): Promise<void> {
    try {
      log.info("Initializing ZFS Verioned PostgreSQL Engine environment...");

      // Check and install system dependencies
      await this.checkSystemDependencies();

      // Setup ZFS pool and datasets
      await this.setupZfsEnvironment();

      // Setup PostgreSQL environment
      await this.setupPostgresEnvironment();

      // Create necessary directories
      await this.createDirectories();

      // Set appropriate permissions
      await this.setPermissions();

      log.success(
        "ZFS Verioned PostgreSQL Engine environment initialized successfully",
      );
    } catch (error) {
      const err = error as Error;
      log.error(`Failed to initialize environment: ${err.message}`);
      throw error;
    }
  }

  /**
   * Check if the environment is properly initialized
   * @returns Promise<{initialized: boolean, issues: string[]}>
   */
  async checkEnvironment(): Promise<
    { initialized: boolean; issues: string[] }
  > {
    const issues: string[] = [];

    try {
      log.info("Checking ZFS Verioned PostgreSQL Engine environment...");

      // Check ZFS pool
      const zfsPoolExists = await this.checkZfsPool();
      if (!zfsPoolExists) {
        issues.push(`ZFS pool '${this.config.zfsPool}' does not exist`);
      }

      // Check PostgreSQL installation
      const postgresInstalled = await this.checkPostgresInstallation();
      if (!postgresInstalled) {
        issues.push("PostgreSQL is not installed or not accessible");
      }

      // Check mount directory
      const mountDirExists = await this.checkMountDirectory();
      if (!mountDirExists) {
        issues.push(`Mount directory '${this.config.mountDir}' does not exist`);
      }

      // Check permissions
      const permissionsOk = await this.checkPermissions();
      if (!permissionsOk) {
        issues.push("Incorrect permissions on mount directory");
      }

      const initialized = issues.length === 0;

      if (initialized) {
        log.success("Environment check passed");
      } else {
        log.warn(`Environment check found ${issues.length} issues`);
      }

      return { initialized, issues };
    } catch (error) {
      const err = error as Error;
      log.error(`Failed to check environment: ${err.message}`);
      issues.push(`Check failed: ${err.message}`);
      return { initialized: false, issues };
    }
  }

  /**
   * Reset the ZFS Verioned PostgreSQL Engine environment
   * @param force - Force reset even if clones exist
   * @returns Promise<void>
   */
  async resetEnvironment(force: boolean = false): Promise<void> {
    try {
      log.info("Resetting ZFS Verioned PostgreSQL Engine environment...");

      if (!force) {
        // Check for active clones
        const clones = await this.listActiveClones();
        if (clones.length > 0) {
          throw new Error(
            `Cannot reset environment: ${clones.length} active clones found. Use --force to override.`,
          );
        }
      }

      // Stop all PostgreSQL instances
      await this.stopAllPostgresInstances();

      // Destroy ZFS datasets (but not the pool)
      await this.destroyZfsDatasets();

      // Recreate base structure
      await this.setupZfsEnvironment();

      log.success(
        "ZFS Verioned PostgreSQL Engine environment reset successfully",
      );
    } catch (error) {
      const err = error as Error;
      log.error(`Failed to reset environment: ${err.message}`);
      throw error;
    }
  }

  /**
   * Check system dependencies
   * @returns Promise<void>
   */
  private async checkSystemDependencies(): Promise<void> {
    log.info("Checking system dependencies...");

    // Check if running as root or with sudo
    const isRoot = await this.checkRootAccess();
    if (!isRoot) {
      throw new Error("Root access required for initialization");
    }

    // Check ZFS availability
    const zfsAvailable = await this.checkZfsAvailability();
    if (!zfsAvailable) {
      throw new Error("ZFS is not available on this system");
    }

    // Check PostgreSQL availability
    const postgresAvailable = await this.checkPostgresAvailability();
    if (!postgresAvailable) {
      throw new Error("PostgreSQL is not available on this system");
    }

    log.success("System dependencies check passed");
  }

  /**
   * Setup ZFS environment
   * @returns Promise<void>
   */
  private async setupZfsEnvironment(): Promise<void> {
    log.info("Setting up ZFS environment...");

    // Check if ZFS pool exists
    const poolExists = await this.checkZfsPool();
    if (!poolExists) {
      throw new Error(
        `ZFS pool '${this.config.zfsPool}' does not exist. Please create it first.`,
      );
    }

    // Create base dataset if it doesn't exist
    const baseDataset = `${this.config.zfsPool}/zvpg`;
    try {
      await runCommand("zfs", ["create", baseDataset]);
      log.success(`Created base dataset: ${baseDataset}`);
    } catch (_error) {
      // Dataset might already exist
      log.info(`Base dataset ${baseDataset} already exists or creation failed`);
    }

    // Set ZFS properties
    await runCommand("zfs", [
      "set",
      "mountpoint=" + this.config.mountDir,
      baseDataset,
    ]);
    await runCommand("zfs", ["set", "compression=lz4", baseDataset]);
    await runCommand("zfs", ["set", "atime=off", baseDataset]);

    log.success("ZFS environment setup completed");
  }

  /**
   * Setup PostgreSQL environment
   * @returns Promise<void>
   */
  private async setupPostgresEnvironment(): Promise<void> {
    log.info("Setting up PostgreSQL environment...");

    // Ensure PostgreSQL user exists
    await this.ensurePostgresUser();

    // Create initial database template
    await this.createInitialTemplate();

    log.success("PostgreSQL environment setup completed");
  }

  /**
   * Create necessary directories
   * @returns Promise<void>
   */
  private async createDirectories(): Promise<void> {
    log.info("Creating necessary directories...");

    const directories = [
      this.config.mountDir,
      `${this.config.mountDir}/${this.config.dataSubdir}`,
      `${this.config.mountDir}/${this.config.clonesSubdir}`,
      `${this.config.mountDir}/${this.config.socketSubdir}`,
      this.config.logDir,
    ];

    for (const dir of directories) {
      await runCommand("mkdir", ["-p", dir]);
    }

    log.success("Directories created successfully");
  }

  /**
   * Set appropriate permissions
   * @returns Promise<void>
   */
  private async setPermissions(): Promise<void> {
    log.info("Setting permissions...");

    // Set ownership to postgres user
    await runCommand("chown", [
      "-R",
      `${this.config.postgresUser}:${this.config.postgresUser}`,
      this.config.mountDir,
    ]);

    // Set directory permissions
    await runCommand("chmod", ["755", this.config.mountDir]);
    await runCommand("chmod", [
      "700",
      `${this.config.mountDir}/${this.config.dataSubdir}`,
    ]);

    log.success("Permissions set successfully");
  }

  /**
   * Check if ZFS pool exists
   * @returns Promise<boolean>
   */
  private async checkZfsPool(): Promise<boolean> {
    try {
      await runCommand("zfs", ["list", this.config.zfsPool]);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Check PostgreSQL installation
   * @returns Promise<boolean>
   */
  private async checkPostgresInstallation(): Promise<boolean> {
    try {
      await runCommand("which", ["psql"]);
      await runCommand("which", ["pg_ctl"]);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Check mount directory
   * @returns Promise<boolean>
   */
  private async checkMountDirectory(): Promise<boolean> {
    try {
      await runCommand("ls", ["-d", this.config.mountDir]);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Check permissions
   * @returns Promise<boolean>
   */
  private async checkPermissions(): Promise<boolean> {
    try {
      const result = await runCommand("stat", [
        "-c",
        "%U:%G",
        this.config.mountDir,
      ]);
      const ownership = result.stdout?.trim();
      return ownership ===
        `${this.config.postgresUser}:${this.config.postgresUser}`;
    } catch {
      return false;
    }
  }

  /**
   * Check root access
   * @returns Promise<boolean>
   */
  private async checkRootAccess(): Promise<boolean> {
    try {
      const result = await runCommand("id", ["-u"]);
      return result.stdout?.trim() === "0";
    } catch {
      return false;
    }
  }

  /**
   * Check ZFS availability
   * @returns Promise<boolean>
   */
  private async checkZfsAvailability(): Promise<boolean> {
    try {
      await runCommand("which", ["zfs"]);
      await runCommand("zfs", ["version"]);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Check PostgreSQL availability
   * @returns Promise<boolean>
   */
  private async checkPostgresAvailability(): Promise<boolean> {
    try {
      await runCommand("which", ["postgres"]);
      await runCommand("postgres", ["--version"]);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * List active clones
   * @returns Promise<string[]>
   */
  private async listActiveClones(): Promise<string[]> {
    try {
      const result = await runCommand("zfs", [
        "list",
        "-r",
        "-t",
        "filesystem",
        "-H",
        "-o",
        "name",
        this.config.zfsPool,
      ]);
      const lines = result.stdout?.trim().split("\n") || [];

      return lines
        .filter((line) => line.trim() && line !== this.config.zfsPool)
        .map((line) => line.replace(`${this.config.zfsPool}/`, ""));
    } catch {
      return [];
    }
  }

  /**
   * Stop all PostgreSQL instances
   * @returns Promise<void>
   */
  private async stopAllPostgresInstances(): Promise<void> {
    log.info("Stopping all PostgreSQL instances...");

    try {
      await runCommand("sudo", [
        "-u",
        this.config.postgresUser,
        "pg_ctl",
        "stop",
        "-m",
        "fast",
      ]);
    } catch (_error) {
      log.warn(
        "Failed to stop PostgreSQL instances gracefully, attempting force stop...",
      );

      try {
        await runCommand("sudo", [
          "-u",
          this.config.postgresUser,
          "pg_ctl",
          "stop",
          "-m",
          "immediate",
        ]);
      } catch (_forceError) {
        log.warn("Failed to force stop PostgreSQL instances");
      }
    }
  }

  /**
   * Destroy ZFS datasets
   * @returns Promise<void>
   */
  private async destroyZfsDatasets(): Promise<void> {
    log.info("Destroying ZFS datasets...");

    try {
      const baseDataset = `${this.config.zfsPool}/zvpg`;
      await runCommand("zfs", ["destroy", "-r", baseDataset]);
      log.success("ZFS datasets destroyed");
    } catch (_error) {
      log.warn("Failed to destroy ZFS datasets - they may not exist");
    }
  }

  /**
   * Ensure PostgreSQL user exists
   * @returns Promise<void>
   */
  private async ensurePostgresUser(): Promise<void> {
    try {
      await runCommand("id", [this.config.postgresUser]);
      log.info(`PostgreSQL user '${this.config.postgresUser}' already exists`);
    } catch {
      log.info(`Creating PostgreSQL user '${this.config.postgresUser}'...`);
      await runCommand("useradd", [
        "-r",
        "-s",
        "/bin/bash",
        this.config.postgresUser,
      ]);
    }
  }

  /**
   * Create initial database template
   * @returns Promise<void>
   */
  private async createInitialTemplate(): Promise<void> {
    log.info("Creating initial database template...");

    const templateDir = `${this.config.mountDir}/template`;

    // Initialize PostgreSQL data directory
    await runCommand("sudo", [
      "-u",
      this.config.postgresUser,
      "initdb",
      "-D",
      templateDir,
      "--auth-host=trust",
      "--auth-local=trust",
    ]);

    log.success("Initial database template created");
  }
}
