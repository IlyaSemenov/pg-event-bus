import { defineConfig } from "tsdown"

export default defineConfig({
  entry: {
    index: "src/index.ts",
    testing: "src/testing.ts",
  },
  format: ["cjs", "esm"],
  dts: true,
  exports: true,
  publint: true,
  attw: {
    profile: "strict",
  },
})
