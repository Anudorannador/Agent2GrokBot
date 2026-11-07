# Grok Bot setup prompt

The canonical setup prompt lives in the skill folder:
[`skills/agent2grokbot/references/bot-setup.md`](../skills/agent2grokbot/references/bot-setup.md).

Read that file and paste the prompt between its markers into the Bot chat once,
when creating the Bot. It installs the standing behaviour for all incoming
webhook tasks. Replace `<PEER_ID>` with the id registered in `peer_add`.

The user (not the Bot) copies the webhook POST URL and sender key from the
routine panel and pastes them into `peer_add`. The Bot never sees those values
and must not invent or ask for them.

No prompt text is duplicated here. The skill folder is the single source of
truth so that a skill-only install and a full repository clone use the same
content.
