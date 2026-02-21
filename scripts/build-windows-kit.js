const { buildWindowsKit } = require("../src/utils/windowsKit");

try {
  const result = buildWindowsKit();
  console.log("Windows kit generated:");
  console.log(`Path: ${result.windowsKitDir}`);
  result.files.forEach((file) => console.log(`- ${file}`));
} catch (err) {
  console.error("Failed to generate Windows kit:", err.message);
  process.exit(1);
}
