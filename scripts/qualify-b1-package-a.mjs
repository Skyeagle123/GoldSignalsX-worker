// Local plan validation only. Deliberately no fetch, Wrangler or resource creation.
import fs from 'node:fs/promises';
import {qualificationPlan,rollbackPlan} from '../measurement-qualification.js';
const [mode='plan',configPath]=process.argv.slice(2);
if(mode==='plan'){const config=JSON.parse(await fs.readFile(configPath??new URL('../deployment/b1-independent/qualification.json',import.meta.url),'utf8'));console.log(JSON.stringify(qualificationPlan(config,config.resources),null,2));}
else if(['CAPTURE','PROCESSING'].includes(mode))console.log(JSON.stringify(rollbackPlan(mode),null,2));
else throw Error('Package A supports plan/rollback-plan only; cloud execution requires Package B');
