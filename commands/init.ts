// ZFS Versioned PostgreSQL Engine - Init Command

import { Command } from "@cliffy/command";
import { log } from "../utils.ts";
import { getConfig, loadConfig } from "../config.ts";
import { InitService } from "../services/init.ts";

const initEnvironmentCommand = new Command()
  .description("Initialize the ZFS Versioned PostgreSQL Engine environment")
  .option("-c, --config <config>", "Configuration file path")
  .option("-f, --force", "Force initialization even if environment exists")
  .action(async (options: { config?: string; force?: boolean }) => {
    await loadConfig(options.config);
    const config = getConfig();
    const initService = new InitService(config);

    try {
      await initService.initializeEnvironment();
      log.success("Environment initialized successfully");
    } catch (error) {
      log.error(error instanceof Error ? error.message : String(error));
      Deno.exit(1);
    }
  });

const checkEnvironmentCommand = new Command()
  .description("Check if the environment is properly initialized")
  .option("-c, --config <config>", "Configuration file path")
  .option("-f, --format <format>", "Output format (table|json)", {
    default: "table",
  })
  .action(async (options: { config?: string; format: string }) => {
    await loadConfig(options.config);
    const config = getConfig();
    const initService = new InitService(config);

    try {
      const result = await initService.checkEnvironment();

      if (options.format === "json") {
        console.log(JSON.stringify(result, null, 2));
      } else {
        if (result.initialized) {
          log.success("Environment is properly initialized");
        } else {
          log.error("Environment is not properly initialized");
          if (result.issues.length > 0) {
            log.info("Issues found:");
            result.issues.forEach((issue) => log.warn(`- ${issue}`));
          }
        }
      }
    } catch (error) {
      log.error(error instanceof Error ? error.message : String(error));
      Deno.exit(1);
    }
  });

export const initCommand = new Command()
  .description("Initialize and manage the ZVPG environment")
  .action(function () {
    this.showHelp();
  })
  .command("env", initEnvironmentCommand)
  .command("check", checkEnvironmentCommand);
