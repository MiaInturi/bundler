import { defineConfig } from 'tsup';

export default defineConfig(options => ({
  ...options,
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  outDir: 'lib',
  clean: true,
  splitting: false,
  treeshake: true,
  minify: true,
  minifyWhitespace: true,
  minifyIdentifiers: true,
  minifySyntax: true,
  dts: true,
}));
