# Ugly Family Mad Libs catalogue v7

The active catalogue is the owner's replacement of all 23 stories, image prompts and ordered question lists in `modules/data/madlibStoriesV7.json`. Each new play asks exactly 10 questions, for 230 catalogue questions total. The [full review copy](madlib-catalogue-v7.md) includes every supplied story and prompt.

Stories 7, 11, 17 and 21 depict one Squig; the other 19 depict two. The private image instructions display that count and ask for the player's Squig as the primary reference. The previous typed cast and multiple numbered attachment requirements do not apply to new plays. No editorial additions are inserted into the owner's image prompts.

Templates use version 7, `format: authored-pair` and `promptFormat: primary-reference`. Numbered placeholders map to the exact ordered word types, preserving literal answers inside the supplied quotation marks. Each story uses all 10 answers, while image prompts use their supplied subset. The single export block reflects the play's saved upload limit, with the owner's 8 MiB default and 4 MiB target when no lower bot limit applies.

Historical catalogue files, including the v6 generator and scene plans, are archived and are not used by the active template loader. The old generator writes only the archived v4/v6 catalogue, never the active v7 file. Existing v1–v6 sessions carry full snapshots: their stories, question counts, prompts and render paths remain intact. Stable story IDs and the last-ten-stories avoidance rule remain in place. No schema, payment, reward or upload processing changes are required.

Tests cover all 23 complete ten-question flows, exact substitution, counts, Unicode and maximum-length answers, Discord output sizes, saved export limits, and historical rendering/resumption. Startup reports `CATALOG_READY 23 STORIES`.
