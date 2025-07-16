// ZFS Verioned PostgreSQL Engine - Utility Functions

import { colors } from "@cliffy/ansi/colors";
import { Config, getConfig } from "./config.ts";

export const log = {
  debug: (message: string) => {
    const config = getConfig();
    if (config.logLevel === "DEBUG") {
      console.log(colors.gray(`[${formatTimestamp()}] [DEBUG] ${message}`));
    }
  },
  info: (message: string) => {
    const config = getConfig();
    if (["DEBUG", "INFO"].includes(config.logLevel)) {
      console.log(colors.blue(`[${formatTimestamp()}] [INFO] ${message}`));
    }
  },
  warn: (message: string) => {
    const config = getConfig();
    if (["DEBUG", "INFO", "WARN"].includes(config.logLevel)) {
      console.log(colors.yellow(`[${formatTimestamp()}] [WARN] ${message}`));
    }
  },
  error: (message: string) => {
    console.error(colors.red(`[${formatTimestamp()}] [ERROR] ${message}`));
  },
  success: (message: string) => {
    console.log(colors.green(`[${formatTimestamp()}] [SUCCESS] ${message}`));
  },
};

export function formatTimestamp(): string {
  return new Date().toISOString().slice(0, 19).replace("T", " ");
}

export function formatISOTimestamp(): string {
  return new Date().toISOString();
}

export async function runCommand(
  command: string,
  args: string[] = [],
  options: {
    cwd?: string;
    env?: Record<string, string>;
    stdout?: "inherit" | "piped" | "null";
    stderr?: "inherit" | "piped" | "null";
  } = {},
): Promise<{ success: boolean; stdout?: string; stderr?: string }> {
  const cmd = new Deno.Command(command, {
    args,
    cwd: options.cwd,
    env: options.env,
    stdout: options.stdout ?? "piped",
    stderr: options.stderr ?? "piped",
  });

  try {
    const { success, stdout, stderr } = await cmd.output();

    return {
      success,
      stdout: stdout ? new TextDecoder().decode(stdout) : undefined,
      stderr: stderr ? new TextDecoder().decode(stderr) : undefined,
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    log.error(`Failed to execute command: ${command} ${args.join(" ")}`);
    log.error(`Error: ${errorMessage}`);
    return { success: false, stderr: errorMessage };
  }
}

export async function checkCommand(command: string): Promise<boolean> {
  const result = await runCommand("which", [command], {
    stdout: "null",
    stderr: "null",
  });
  return result.success;
}

export function validateBranchName(name: string): boolean {
  const config = getConfig();
  const pattern = new RegExp(config.branchNamingPattern, "u");
  return pattern.test(name);
}

export function validateSnapshotName(name: string): boolean {
  const pattern = /^[a-zA-Z0-9_-]+$/;
  return pattern.test(name);
}

export function validatePort(port: number): boolean {
  const config = getConfig();
  return port >= config.branchPortStart && port <= config.branchPortEnd;
}

export async function pickValidPort(
  config: Config,
  port?: number,
): Promise<number> {
  if (port && validatePort(port)) {
    const inUse = await isPortInUse(port);
    if (!inUse) {
      return port;
    }
    throw new Error(`Port ${port} is already in use`);
  }

  for (let p = config.branchPortStart; p <= config.branchPortEnd; p++) {
    const inUse = await isPortInUse(p);
    if (!inUse) {
      return p;
    }
  }

  throw new Error(
    `No available ports in range ${config.branchPortStart}-${config.branchPortEnd}`,
  );
}

export async function snapshotExists(snapshotName: string): Promise<boolean> {
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

export async function datasetExists(datasetName: string): Promise<boolean> {
  const result = await runCommand("zfs", ["list", datasetName], {
    stdout: "null",
    stderr: "null",
  });
  return result.success;
}

export async function getLatestSnapshot(): Promise<string | null> {
  const config = getConfig();
  const result = await runCommand("zfs", [
    "list",
    "-t",
    "snapshot",
    "-r",
    `${config.zfsPool}/${config.dataSubdir}`,
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

export async function getNextPort(): Promise<number> {
  const config = getConfig();

  for (
    let port = config.branchPortStart;
    port <= config.branchPortEnd;
    port++
  ) {
    const isInUse = await isPortInUse(port);
    if (!isInUse) {
      return port;
    }
  }

  throw new Error(
    `No available ports in range ${config.branchPortStart}-${config.branchPortEnd}`,
  );
}

export async function isPortInUse(port: number): Promise<boolean> {
  const result = await runCommand("netstat", ["-tuln"], { stdout: "piped" });

  if (!result.success || !result.stdout) {
    return false;
  }

  return result.stdout.includes(`:${port} `);
}

export function getZfsCloneName(port: number): string {
  const config = getConfig();
  return `${config.zfsPool}/${config.clonesSubdir}/clone_${port}`;
}

export function getCloneMount(port: number): string {
  const config = getConfig();
  return `${config.mountDir}/${config.zfsPool}/${config.clonesSubdir}/clone_${port}`;
}

export function getBranchDataset(branchName: string): string {
  const config = getConfig();
  return `${config.zfsPool}/branches/${branchName}`;
}

export function getBranchMount(branchName: string): string {
  const config = getConfig();
  return `${config.mountDir}/${config.zfsPool}/branches/${branchName}`;
}

export function expandPath(path: string): string {
  if (path.startsWith("~/")) {
    const homeDir = Deno.env.get("HOME") || "/root";
    return path.replace("~", homeDir);
  }
  return path;
}

export function getContainerName(branchName: string, port: number): string {
  return `zvpg_${branchName}_${port}`;
}

export async function checkContainerRuntime(runtime: string): Promise<boolean> {
  return await checkCommand(runtime);
}

export async function isContainerRunning(
  containerName: string,
  runtime: string,
): Promise<boolean> {
  const result = await runCommand(runtime, [
    "ps",
    "-q",
    "-f",
    `name=${containerName}`,
  ], {
    stdout: "piped",
    stderr: "null",
  });
  return result.success && result.stdout?.trim() !== "";
}

export async function stopContainer(
  containerName: string,
  runtime: string,
): Promise<void> {
  const isRunning = await isContainerRunning(containerName, runtime);
  if (isRunning) {
    log.info(`Stopping container: ${containerName}`);
    await runCommand(runtime, ["stop", containerName]);
    await runCommand(runtime, ["rm", containerName]);
  }
}
