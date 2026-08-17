/**
 * Run libibt tests in Pyodide (WebAssembly) environment.
 *
 * Usage: node scripts/run_pyodide_tests.mjs [--dist-dir=./dist] [--pyodide-version=0.29.3]
 */

import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(__dirname, "..");
const pyodideTestsDir = path.join(__dirname, "pyodide_tests");

/**
 * Recursively copy a directory to the Pyodide virtual filesystem.
 */
function copyDirToFs(pyodide, srcDir, dstDir) {
  if (!fs.existsSync(srcDir)) {
    console.log(`Warning: Source directory not found: ${srcDir}`);
    return;
  }

  try {
    pyodide.FS.mkdir(dstDir);
  } catch (e) {
    // Directory might already exist
  }

  const entries = fs.readdirSync(srcDir, { withFileTypes: true });
  for (const entry of entries) {
    const srcPath = path.join(srcDir, entry.name);
    const dstPath = `${dstDir}/${entry.name}`;

    if (entry.isDirectory()) {
      copyDirToFs(pyodide, srcPath, dstPath);
    } else if (entry.isFile() || entry.isSymbolicLink()) {
      const data = fs.readFileSync(srcPath);
      pyodide.FS.writeFile(dstPath, data);
    }
  }
}

// Pyodide runtime version -> wheel ABI tag. One entry per supported runtime.
const ABI_TAG = {
  "0.29": "pyodide_2025",     // pre-PEP-783 tag, GitHub Releases only
  "314": "pyemscripten_2026", // PEP 783 tag (Python 3.14), published to PyPI
};

// "0.29.3" -> "0.29", "314.0.4" -> "314". Keys ABI_TAG and the npm package dir.
function runtimeSeries(version) {
  return version.startsWith("314") ? "314" : version.substring(0, 4);
}

/**
 * Find the Pyodide-compatible wheel file in the dist directory.
 * @param {string} distDir - Directory containing wheel files
 * @param {string} pyodideVersion - Pyodide runtime version (e.g. "0.29.3")
 */
function findWheel(distDir, pyodideVersion) {
  if (!fs.existsSync(distDir)) {
    throw new Error(`Dist directory not found: ${distDir}`);
  }

  const wheels = fs.readdirSync(distDir).filter((f) => f.endsWith(".whl"));
  if (wheels.length === 0) {
    throw new Error(`No wheel files found in ${distDir}`);
  }

  // Determine ABI tag based on version
  const abiTag = ABI_TAG[runtimeSeries(pyodideVersion)] ?? "pyodide_2025";

  // Find wheel matching the ABI tag
  const matchingWheel = wheels.find((w) => w.includes(abiTag));
  if (matchingWheel) {
    return path.join(distDir, matchingWheel);
  }

  throw new Error(
    `No wheel with ABI tag ${abiTag} found in ${distDir}. Found: ${wheels.join(", ")}`
  );
}

/**
 * Parse command line arguments.
 */
function parseArgs() {
  const args = {
    distDir: path.join(projectRoot, "dist"),
    pyodideVersion: "0.29.3",
  };

  for (const arg of process.argv.slice(2)) {
    if (arg.startsWith("--dist-dir=")) {
      args.distDir = arg.split("=")[1];
    } else if (arg.startsWith("--pyodide-version=")) {
      args.pyodideVersion = arg.split("=")[1];
    }
  }

  return args;
}

/**
 * Load the Pyodide npm package matching the requested runtime version.
 * @param {string} version - Pyodide version (e.g., "0.29.3")
 */
async function loadPyodideModule(version) {
  const mod = await import(`pyodide-${runtimeSeries(version)}`);
  return mod.loadPyodide;
}

async function main() {
  const args = parseArgs();

  console.log(`Loading Pyodide ${args.pyodideVersion}...`);
  const loadPyodide = await loadPyodideModule(args.pyodideVersion);
  const pyodide = await loadPyodide();

  console.log("Loading packages...");
  await pyodide.loadPackage(["numpy", "pyarrow", "micropip"]);

  // Find and install the wheel
  const wheelPath = findWheel(args.distDir, args.pyodideVersion);
  console.log(`Installing wheel: ${wheelPath}`);

  const micropip = pyodide.pyimport("micropip");
  await micropip.install(`file://${path.resolve(wheelPath)}`);

  // Copy test directory to Pyodide filesystem
  const testsDir = path.join(projectRoot, "tests");
  console.log(`Copying test files from ${testsDir}...`);
  copyDirToFs(pyodide, testsDir, "/tests");

  // Create pyodide_tests directory and copy Python test runner
  try {
    pyodide.FS.mkdir("/pyodide_tests");
  } catch (e) {
    // Directory might already exist
  }

  console.log("\nRunning tests...\n");

  // Read and execute Python test runner
  const testRunnerPath = path.join(pyodideTestsDir, "run_unit_tests.py");
  const testRunnerCode = fs.readFileSync(testRunnerPath, "utf-8");
  pyodide.FS.writeFile("/pyodide_tests/run_unit_tests.py", testRunnerCode);

  const testResult = await pyodide.runPythonAsync(`
import sys
sys.path.insert(0, '/pyodide_tests')
from run_unit_tests import run_tests
run_tests()
`);

  console.log(`\nTest result: ${testResult === 0 ? "PASSED" : "FAILED"}`);
  process.exit(testResult);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
