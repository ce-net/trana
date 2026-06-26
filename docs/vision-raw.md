# trana — the founder's raw words

This is the unedited, verbatim record of everything Leif said while specifying trana, in the order he
said it. Spelling, phrasing, and typos are preserved exactly — this is the source-of-truth product
vision in his own voice, not a paraphrase. Each entry notes only the conversational context in
brackets; the quoted text is raw.

Captured 2026-06-26.

---

## 1 — The brief [initial request]

> Make a new social media app called trana. It connects your ce-iam and profile and lets you upload
> content, videos, stream, audio and on threads like reddit you can discuss things. the point is to
> build trust - karma system like reddit - with trust people are more likely to give you criticial
> tasks. On your profile your nodes and devices uptime and compute capacity and average price etc
> should all be avilable via api. start with making the distributed backends for all of the content
> we will be serving - threads, video streaming, live streaming, audio, podcasts, images...
> everuthing 100% distributed, replicated with wasm support so mobile devices can also contribute to
> it. Make the backend first - no frontend. fully distributed and auto spawning on closeby nodes...
> give it a nice api. let people build their profiles using its api. This is a core component and
> will note only be used for one frontend social media app but many different - this is a reusable
> component which everyone can use for referencing and building trust profiles for whom to trust -
> and im seeding it with my videos, live streaming, documents and other files and contents during
> ce-net development.

## 2 — Testing [next request]

> write e2e tests over real vms with the whole ce ecosystem - verify that ce node setup works and
> that ce-gke works with what youve built and that all systems really works and talks to eachtoehr
> and is distributed correctly and is fault tolerant when nodes fail randomly

## 3 — [interjection]

> continue

## 4 — [interjection]

> ce-gke.

## 5 — [interjection, confirming ce-gke is real]

> it exists.

## 6 — Product questions / expand [request]

> Alright nice what contents work? does all content types work? Do we have karma and voting? Who gets
> access to voting? Reporting and trust? How does banning work? who controls it? And more... expand
> backend

## 7 — [interjection]

> upvotes and downvotes

## 8 — [interjection]

> feed algorithm

## 9 — [interjection]

> hot, trending...

## 10 — The governance model [the core decision]

> Its community voting. if people dont like you, if you push controversal opinions - your should
> first get more visibility to give a change to change people and find people who think like you -
> but if you continue to get boed down then there are votes and youre banned. there are no mods like
> in reddit - only community voting, trust, respect and later we will have ai policy making sure all
> our policies are followed - policies voted forward by people.

## 11 — [this document]

> Document everything ive said, raw

---

## Decision log (choices made, not just said)

- When asked to choose a moderation authority model, Leif first selected **"Hybrid"** (board owners
  delegate mods + per-node override + trust-gating). He then **overrode it** in message #10 above:
  **no mods at all — pure community voting, trust, respect, and future AI policy enforcement of
  community-voted policies.** Message #10 is the binding word; the Hybrid pick is superseded.
