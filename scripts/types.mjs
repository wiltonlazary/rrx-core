// Generates the published declarations: `tsc` emits .d.ts files from the
// JSDoc of src/ into types/ (the package's `types` entries).
import { execFileSync } from 'node:child_process';
import { rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';

const here = fileURLToPath(new URL('..', import.meta.url));
const require = createRequire(import.meta.url);
rmSync(`${here}types`, { recursive: true, force: true });
const tsc = `${dirname(require.resolve('typescript/package.json'))}/bin/tsc`;
execFileSync(process.execPath, [tsc, '-p', `${here}tsconfig.types.json`], { stdio: 'inherit' });
