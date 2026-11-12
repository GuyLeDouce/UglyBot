# Ugly Family Mad Libs catalogue v5

The active catalogue contains exactly the 23 stories supplied by the owner, stored in `modules/data/madlibStoriesV4.json`. That file preserves each story, image prompt and ordered word-prompt list. New plays no longer select from the previous 60 human-world scenes. Historical source files remain archived; sessions store full template snapshots and do not resolve their stories from the active catalogue.

The first three stories ask 14 questions each; the remaining 20 ask 13, for 302 prompts total. Repeated numbered blanks reuse the same answer. Distinct blanks of the same type remain separate questions. The user answers one question at a time with Back and Exit / Save available. The story remains hidden until completion.

The authored-pair format uses `format: authored-pair` with independent story and image text. Every story uses every answer; an image uses the subset explicitly supplied by its author. Version 5 uses the owner’s rewritten complete reference prompts, keeping the attached Squig as the focal character and forbidding new lettering. A single export block reflects the saved output budget; the supplied default is unchanged. Numbered answers substitute inside the authored quotes without adding another pair. The bot provides prompts; it does not generate the illustrations itself.

No schema migration, history deletion, payment change or reward change is required. Fictional prizes and events in story text do not initiate any $CHARM payment. Previous v1–v4 saved plays retain their text, questions and render path. Templates retain IDs `ugly-family-01` through `ugly-family-23`, version 5, and the existing last-ten-stories avoidance rule.

Validation covers ordered keys, missing/unknown placeholders, exact authored substitution, duplicate identities, long and Unicode answers, Discord payload budgets, every complete question flow, and legacy saved-session rendering and resumption. Startup logs `CATALOG_READY 23 STORIES` when initialization succeeds.
