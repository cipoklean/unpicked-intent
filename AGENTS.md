# AGENTS.md — read this first (Devin, Junie, any agent)

This repo is built by one human (David / cipoklean) working with three AI
agents. None of them share memory, so **this file and `docs/STATUS.md` are
the shared memory.** Read both before doing anything.

## Who does what
| Agent | Role |
|---|---|
| **Devin** | Infrastructure: repo scaffolding, CI, hooks, hosting/deploy, packaging. |
| **Junie** (in PyCharm via JetBrains Air) | Feature code and tests inside the IDE. This is the JetBrains tool the hackathon judges. |
| **Hark** | Planning, data analysis on the raw GRID data (outside this repo), reviewing every push, writing Junie's prompts. |

## Rules every agent follows
1. **License boundary.** "Could this file exist without the GRID feed?" If no, it never goes in this repo. Raw feeds → `data/`, generated artifacts → `derived/` (both gitignored). `static/champion_roles.csv` is the only committed CSV. See `.junie/guidelines.md` and `BUILD.md`.
2. **Commits are authored and committed as `cipoklean <aghaziedavid@gmail.com>` only.** No bot identities, no `Co-authored-by` trailers.
3. Tests use synthetic fixtures under `tests/fixtures/`, never feed data.
4. `BUILD.md` is the spec; locked decisions there aren't reopened without David.
5. **When you finish a task, append one entry to `docs/STATUS.md`** (date, agent, what changed, what's next). That's how the others find out.
6. Keep `.idea/` and `.junie/` committed (hackathon evidence of JetBrains use).
