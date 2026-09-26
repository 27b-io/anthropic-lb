# Changelog

## [0.2.6](https://github.com/27b-io/anthropic-lb/compare/v0.2.5...v0.2.6) (2026-09-26)


### Features

* **auth:** attribute credential rejections beyond the client IP (LAB-4720) ([#208](https://github.com/27b-io/anthropic-lb/issues/208)) ([98b8618](https://github.com/27b-io/anthropic-lb/commit/98b8618b3c70cd31518dc8fda83b80f6b458dfcf))
* **LAB-2551:** count pre-request-gate 429 rejections by client and reason ([#150](https://github.com/27b-io/anthropic-lb/issues/150)) ([b847800](https://github.com/27b-io/anthropic-lb/commit/b847800f62746a093596c9e16dfd8127fc11c5c0))
* **LAB-4189:** export per-claim rate-limit status, reset, and pool-exhaustion count ([#191](https://github.com/27b-io/anthropic-lb/issues/191)) ([9c0d22b](https://github.com/27b-io/anthropic-lb/commit/9c0d22b9aa2d8027cde27f1be250da48118a3e33))
* **LAB-4379:** export request latency, process start time and build identity on /metrics ([#195](https://github.com/27b-io/anthropic-lb/issues/195)) ([a334cb3](https://github.com/27b-io/anthropic-lb/commit/a334cb3f17be639ff2c3feb997cc115b68b0b8af))
* **LAB-4395:** add a read-only principal for /_stats and /metrics ([#196](https://github.com/27b-io/anthropic-lb/issues/196)) ([a8d394c](https://github.com/27b-io/anthropic-lb/commit/a8d394c6dc11d52ef7e96091c0e81240a04a256b))
* **openai-compat:** translate reasoning_effort to output_config.effort (LAB-5121) ([#213](https://github.com/27b-io/anthropic-lb/issues/213)) ([6433f77](https://github.com/27b-io/anthropic-lb/commit/6433f773c56fd0d6dd4f39c8aba817362c22b2ae))


### Bug Fixes

* **beta:** strip orphaned body fields when the beta allow-list drops a flag (LAB-1261) ([#192](https://github.com/27b-io/anthropic-lb/issues/192)) ([c091990](https://github.com/27b-io/anthropic-lb/commit/c091990ab5ceeec814d04012aaa9a3ac14290e4d))
* **budget:** recover poisoned budget_usage lock instead of failing open (LAB-4315) ([#216](https://github.com/27b-io/anthropic-lb/issues/216)) ([6cac7f3](https://github.com/27b-io/anthropic-lb/commit/6cac7f3a4eb8079f0741eeb8ed8516bb0c36309e))
* **config:** reject admin_readers nested under any table (LAB-4395) ([#229](https://github.com/27b-io/anthropic-lb/issues/229)) ([842eebc](https://github.com/27b-io/anthropic-lb/commit/842eebc3d6180dfada0373abd77ca6c5e1aea553))
* **LAB-2299:** reserve the killable proxy's port so the revive bind cannot race ([#165](https://github.com/27b-io/anthropic-lb/issues/165)) ([e9824c9](https://github.com/27b-io/anthropic-lb/commit/e9824c93ad95030e5991438589dfcf36c385de88))
* **LAB-3295:** status-floor-bound affinity migrations log at INFO, not WARN ([#175](https://github.com/27b-io/anthropic-lb/issues/175)) ([3931fac](https://github.com/27b-io/anthropic-lb/commit/3931fac66292b372d4e79e59d5c80040175b005b))
* **LAB-3964:** allow the Claude Code per-turn beta family through the OAuth allow-list ([#186](https://github.com/27b-io/anthropic-lb/issues/186)) ([e84a44c](https://github.com/27b-io/anthropic-lb/commit/e84a44cc21c717bbb0e86efc82b2ee9ff3c68e6d))
* **LAB-4031:** enforce single-terminator SSE invariant on all four stream loops ([#184](https://github.com/27b-io/anthropic-lb/issues/184)) ([e2bddb5](https://github.com/27b-io/anthropic-lb/commit/e2bddb5d4e7a6747472160c67390270e0b67a169))
* **LAB-4127:** keep Claude Code attribution block first when injecting the OAuth prompt ([#188](https://github.com/27b-io/anthropic-lb/issues/188)) ([ebb5465](https://github.com/27b-io/anthropic-lb/commit/ebb546548a4e3a5fe3d0f2d3e2b5ed01c38ba89e))
* **LAB-4322:** fail closed when OpenAI `messages` is not an array ([#194](https://github.com/27b-io/anthropic-lb/issues/194)) ([62c8a47](https://github.com/27b-io/anthropic-lb/commit/62c8a47604eb914a182854992b3436a7dec3eff1))
* **LAB-4358:** fail closed on a `messages` the guard cannot read ([#205](https://github.com/27b-io/anthropic-lb/issues/205)) ([7b07f04](https://github.com/27b-io/anthropic-lb/commit/7b07f047f8f162f0aa12b75577363d412eae7205))
* **LAB-4719:** sticky migration never lands on an account it would itself flee ([#207](https://github.com/27b-io/anthropic-lb/issues/207)) ([0716ba4](https://github.com/27b-io/anthropic-lb/commit/0716ba4c9d1eb585c71d69b5f16ae009aaee0192))
* **LAB-4729:** re-send once on an upstream "out of extra usage" 400 ([#209](https://github.com/27b-io/anthropic-lb/issues/209)) ([6aa1685](https://github.com/27b-io/anthropic-lb/commit/6aa16859b573c62f69261b0447ab793fea365fb0))
* **LAB-4814:** give every redis_integration test its own logical DB ([#211](https://github.com/27b-io/anthropic-lb/issues/211)) ([5980076](https://github.com/27b-io/anthropic-lb/commit/59800769a7c1b16914f8eea6e78edab1ad65dc8a))
* **LAB-5166:** recover poisoned std Mutex locks instead of skipping them ([#218](https://github.com/27b-io/anthropic-lb/issues/218)) ([68fbfd6](https://github.com/27b-io/anthropic-lb/commit/68fbfd6e2b3b7551086113769c5da47f91c0aac6))
* **LAB-5235:** match the free-text model-rejection arm on OpenAI endpoints only ([#221](https://github.com/27b-io/anthropic-lb/issues/221)) ([097f264](https://github.com/27b-io/anthropic-lb/commit/097f264b73d7560c616116f1a685fc8d28051f9c))
* **LAB-5250:** move coherent_body debug_assert to the non-OAuth arm ([#222](https://github.com/27b-io/anthropic-lb/issues/222)) ([7426d18](https://github.com/27b-io/anthropic-lb/commit/7426d18f39c6076805bd6d35117eba879c25fea3))
* **LAB-5278:** stop echoed request keys from reading as account or model state ([#228](https://github.com/27b-io/anthropic-lb/issues/228)) ([4e7c484](https://github.com/27b-io/anthropic-lb/commit/4e7c484da5275b93c246ddac79f4f805eabe6c7d))
* **LAB-5313:** report the real outcome on four swallowed error paths ([#226](https://github.com/27b-io/anthropic-lb/issues/226)) ([0f7551d](https://github.com/27b-io/anthropic-lb/commit/0f7551d81bf832912fcd9866384ea9a7aa5d4992))
* **metrics:** publish the overage gate the router uses (LAB-4441) ([#206](https://github.com/27b-io/anthropic-lb/issues/206)) ([07160df](https://github.com/27b-io/anthropic-lb/commit/07160df62d582964d10949da2a16985d4ea388fd))
* propagate in-band upstream SSE errors in OpenAI-compat translators (LAB-710) ([#136](https://github.com/27b-io/anthropic-lb/issues/136)) ([d07d1ad](https://github.com/27b-io/anthropic-lb/commit/d07d1ada98a61030af9c635c83dcb807b0cf31ac))
* reflect upstream x-should-retry header (LAB-4128) ([#187](https://github.com/27b-io/anthropic-lb/issues/187)) ([4abe23b](https://github.com/27b-io/anthropic-lb/commit/4abe23b5b3e667047e87b2f7cb587d017de2218c))
* reject non-object JSON bodies with a 400 instead of panicking (LAB-4314) ([#193](https://github.com/27b-io/anthropic-lb/issues/193)) ([6d88121](https://github.com/27b-io/anthropic-lb/commit/6d88121b05b5e1b5c05eaf4ce6e6432f120af0d6))
* **routing:** stop re-sending applied transport-error counts on partial pipeline failure (LAB-5446) ([#233](https://github.com/27b-io/anthropic-lb/issues/233)) ([021c8ce](https://github.com/27b-io/anthropic-lb/commit/021c8ce05f59ed3ce6b3e2c05dcf8432014bc7e6))

## [0.2.5](https://github.com/27b-io/anthropic-lb/compare/v0.2.4...v0.2.5) (2026-09-19)


### ⚠ BREAKING CHANGES

* **LAB-3214:** log consumers filtering on `usage` or `fingerprint` at INFO must filter on `proxied` (or `proxied (openai-compat)`) instead. The `usage` line is merged into one `proxied` line per request, emitted at completion with the token fields (names unchanged). Error paths log it with zeroed usage, except an interrupted stream (client disconnect or mid-stream upstream error), which logs the usage captured before the interruption and so can carry nonzero `input`. `usage` survives only for requests routed to a `protocol = "openai"` endpoint, from either `/v1/messages` or `/v1/chat/completions`; those keep a usage-only echo (non-streaming responses with non-empty usage) — keep the `usage` filter if you route to such endpoints. The `fingerprint` line is DEBUG-only; its `fp` rides on the native `/v1/messages` `proxied` line via [#174](https://github.com/27b-io/anthropic-lb/issues/174) (the openai-compat line carries no `fp`). Entry restored by hand: the malformed commit subject (`fix!(scope):`) hid this change from release-please, hence a patch release. ([#172](https://github.com/27b-io/anthropic-lb/issues/172)) ([a0d0269](https://github.com/27b-io/anthropic-lb/commit/a0d0269d391c9d22144fa38ebebd338bd31db534))


### Features

* **guard:** Tier 0 request content-scan layer (LAB-3877) ([#179](https://github.com/27b-io/anthropic-lb/issues/179)) ([9a97ebb](https://github.com/27b-io/anthropic-lb/commit/9a97ebb9a5a4b36be52bc83fdcd2c02b8ae42504))
* **LAB-2636:** per-client endpoint pinning via preferred_endpoints with overage spill-over ([#152](https://github.com/27b-io/anthropic-lb/issues/152)) ([1a02002](https://github.com/27b-io/anthropic-lb/commit/1a02002789acbc4d7614c2bbcb48ad003764a38e))
* **LAB-2669:** allow fast-mode-* client beta through the OAuth allow-list ([#154](https://github.com/27b-io/anthropic-lb/issues/154)) ([32f6ee1](https://github.com/27b-io/anthropic-lb/commit/32f6ee1bc9d69dbd90cc6e9463845fd6503a74ef))
* **LAB-3287:** carry fp on the merged proxied INFO line ([#174](https://github.com/27b-io/anthropic-lb/issues/174)) ([a67b3bd](https://github.com/27b-io/anthropic-lb/commit/a67b3bd22ea2ca630260229dbb24122113f2a2fd))


### Bug Fixes

* **headers:** strip caller-identity headers from upstream requests by default (LAB-3030) ([#170](https://github.com/27b-io/anthropic-lb/issues/170)) ([a5d30ee](https://github.com/27b-io/anthropic-lb/commit/a5d30ee2db4bd432b8e139b50e51f8258198180a))
* **LAB-2332:** hard-bound model_denied overflow to a global bucket ([#148](https://github.com/27b-io/anthropic-lb/issues/148)) ([bf83116](https://github.com/27b-io/anthropic-lb/commit/bf831168d777350cb8bd34e032d8f133c94c900a))
* **LAB-2675:** forward a fast-mode 429 instead of hard-limiting the whole account ([#161](https://github.com/27b-io/anthropic-lb/issues/161)) ([1cac9ff](https://github.com/27b-io/anthropic-lb/commit/1cac9ff1a25231c34ad12fedeb1dec4ff23569d0))
* **LAB-2684:** break affinity on time-free headroom, not waste_risk weight ([#158](https://github.com/27b-io/anthropic-lb/issues/158)) ([a9f9702](https://github.com/27b-io/anthropic-lb/commit/a9f97021765058966b3782e6364eb5c380c7f8f7))
* **LAB-2693:** a fast-mode 200 no longer corrupts the account's headroom view ([#177](https://github.com/27b-io/anthropic-lb/issues/177)) ([0de4ee7](https://github.com/27b-io/anthropic-lb/commit/0de4ee7c9e9c9fae88203e53aafd1fb6a9a0ec10))
* **LAB-3026:** fail startup on unparseable redis_url instead of running local-only ([#169](https://github.com/27b-io/anthropic-lb/issues/169)) ([788e5e7](https://github.com/27b-io/anthropic-lb/commit/788e5e79dbf738e91d3466c3940c4ed55fc62412))
* **LAB-3217:** seed the budget mirror from the shared Redis counter every sync tick ([#173](https://github.com/27b-io/anthropic-lb/issues/173)) ([339b492](https://github.com/27b-io/anthropic-lb/commit/339b4920ea894dc863918771ab86f96b74ee6e4d))
* **LAB-3963:** allow the auto-mode classifier beta pair through the OAuth allow-list ([#181](https://github.com/27b-io/anthropic-lb/issues/181)) ([2e11082](https://github.com/27b-io/anthropic-lb/commit/2e1108215ebf738b1fa75885542ef722a4625506))
* **LAB-798:** drop deprecated temperature for Claude &gt;= 4.7 in OpenAI shim ([#149](https://github.com/27b-io/anthropic-lb/issues/149)) ([1f6ce62](https://github.com/27b-io/anthropic-lb/commit/1f6ce62494f0483e04f987282f4024592598d367))

## [0.2.4](https://github.com/27b-io/anthropic-lb/compare/v0.2.3...v0.2.4) (2026-08-29)


### Features

* **LAB-2330:** per-client token usage by model ([#146](https://github.com/27b-io/anthropic-lb/issues/146)) ([de5d597](https://github.com/27b-io/anthropic-lb/commit/de5d59738bcfff42d03291254349c77597e088b6))


### Bug Fixes

* **LAB-1193:** keep valid auth live behind shared-IP throttle ([#140](https://github.com/27b-io/anthropic-lb/issues/140)) ([5dadee8](https://github.com/27b-io/anthropic-lb/commit/5dadee82b22c22b868d9a2a9254a1de314d17e17))
* **LAB-1962:** stop erasing the shared budget counter on INCRBY failure ([#138](https://github.com/27b-io/anthropic-lb/issues/138)) ([6a1a75e](https://github.com/27b-io/anthropic-lb/commit/6a1a75eadd036339c6aafd6d45d530c43c77d40a))
* **LAB-2214:** box large Err payloads for clippy 1.98 result_large_err ([#143](https://github.com/27b-io/anthropic-lb/issues/143)) ([15bb7f2](https://github.com/27b-io/anthropic-lb/commit/15bb7f23aef00b9a2cd7a7503d51fa6a00c99671))


### Performance Improvements

* **LAB-716:** build OpenAI-path request bodies once per request, not per retry attempt ([#130](https://github.com/27b-io/anthropic-lb/issues/130)) ([6e758f1](https://github.com/27b-io/anthropic-lb/commit/6e758f1beafd2b2f193d133a02e5849b3269c697))

## [0.2.3](https://github.com/27b-io/anthropic-lb/compare/v0.2.2...v0.2.3) (2026-08-08)


### Features

* add /metrics Prometheus endpoint ([#19](https://github.com/27b-io/anthropic-lb/issues/19)) ([b5c4b87](https://github.com/27b-io/anthropic-lb/commit/b5c4b8733b674bf5963fdde9298cd444ca8f59e2))
* add rate-limit status, effective gate, and data age Prometheus metrics ([#53](https://github.com/27b-io/anthropic-lb/issues/53)) ([400e7e0](https://github.com/27b-io/anthropic-lb/commit/400e7e05ad2ff015a542c1ba687d130313394507))
* add reset countdown and account-level waste risk Prometheus metrics ([#48](https://github.com/27b-io/anthropic-lb/issues/48)) ([c1ed8e3](https://github.com/27b-io/anthropic-lb/commit/c1ed8e3ab9e8180986d7e500745c0916ebe5a877))
* add tool calling to OpenAI compat layer ([#22](https://github.com/27b-io/anthropic-lb/issues/22)) ([02ae5e7](https://github.com/27b-io/anthropic-lb/commit/02ae5e7839acb99ccfebcd1240937db969d524a7))
* distributed state via Redis ([#13](https://github.com/27b-io/anthropic-lb/issues/13)) ([2779092](https://github.com/27b-io/anthropic-lb/commit/2779092275360f3366d08dc21c09df23aef547b6))
* fold content fingerprint into the affinity key (distribute fan-outs) ([#83](https://github.com/27b-io/anthropic-lb/issues/83)) ([82be81f](https://github.com/27b-io/anthropic-lb/commit/82be81f906114556d41af956420a2c0253b7fb88))
* log probe weights and deduplicate probes across pods ([#43](https://github.com/27b-io/anthropic-lb/issues/43)) ([35082e3](https://github.com/27b-io/anthropic-lb/commit/35082e32d5fbdf391b1e5fc126ac75469eedb549))
* overage-aware routing with unified endpoint priority ([#59](https://github.com/27b-io/anthropic-lb/issues/59)) ([bde8f1f](https://github.com/27b-io/anthropic-lb/commit/bde8f1f3e61d4d627efd51b02b31adfa5d0bec4f))
* priority tiers and upstream fallback ([#58](https://github.com/27b-io/anthropic-lb/issues/58)) ([d94aec8](https://github.com/27b-io/anthropic-lb/commit/d94aec894281b6b152e771a2e18353419732c7f6))
* support multiple operator client IDs ([#14](https://github.com/27b-io/anthropic-lb/issues/14)) ([5ba63d3](https://github.com/27b-io/anthropic-lb/commit/5ba63d3461d3665342a29ccd73df53d423d6dbc5))
* token budget backpressure with per-claim 7d routing ([#10](https://github.com/27b-io/anthropic-lb/issues/10)) ([e313e6e](https://github.com/27b-io/anthropic-lb/commit/e313e6e594433b7e95836e3470f643d6355ed18f))
* transport circuit-breaker for persistently-dead endpoints ([#70](https://github.com/27b-io/anthropic-lb/issues/70)) ([#89](https://github.com/27b-io/anthropic-lb/issues/89)) ([1c3e6d9](https://github.com/27b-io/anthropic-lb/commit/1c3e6d965b5313570ec3a89c8b5c2886e8ffcc71))
* unified endpoints — collapse accounts + upstreams into one schema ([#61](https://github.com/27b-io/anthropic-lb/issues/61)) ([562420f](https://github.com/27b-io/anthropic-lb/commit/562420fcb24ad42e88b85df9ed0fc6e5e77b32bd))
* upstream transient-failure resilience (round-gated retry, 503, transport-error metric) ([#72](https://github.com/27b-io/anthropic-lb/issues/72)) ([742fc15](https://github.com/27b-io/anthropic-lb/commit/742fc1580458c661e921072a5fd86f1b9bbc04c3))
* waste-risk routing for 7d quota maximization ([#16](https://github.com/27b-io/anthropic-lb/issues/16)) ([3fb4fb9](https://github.com/27b-io/anthropic-lb/commit/3fb4fb9cd1431ae6040816cc595470cbcbf03a49))


### Bug Fixes

* 429 recovery and burst rate-limit detection ([#17](https://github.com/27b-io/anthropic-lb/issues/17)) ([4867b59](https://github.com/27b-io/anthropic-lb/commit/4867b59a71ba5ffe667896f4214efbdee3b89abb))
* add request-balance override for affinity with few clients ([#30](https://github.com/27b-io/anthropic-lb/issues/30)) ([24047d9](https://github.com/27b-io/anthropic-lb/commit/24047d9ab7d1eb825327ffaac4adaa9260914706))
* address bug hunt findings ([#37](https://github.com/27b-io/anthropic-lb/issues/37)) ([d333e7e](https://github.com/27b-io/anthropic-lb/commit/d333e7ec87b4f57f35522f6eaa1c03aa648cdc20))
* BEBO retry on 529 overloaded responses ([#31](https://github.com/27b-io/anthropic-lb/issues/31)) ([5ebfb6f](https://github.com/27b-io/anthropic-lb/commit/5ebfb6f72d9848215b164fee78cfa93901d0bf93))
* clamp legacy utilization fallback, correct stale defaults ([#15](https://github.com/27b-io/anthropic-lb/issues/15)) ([89b4231](https://github.com/27b-io/anthropic-lb/commit/89b42316777947fe53094b9d199b262e4d640f79))
* close idle inbound keep-alive conns to avoid client socket-reuse races ([#65](https://github.com/27b-io/anthropic-lb/issues/65)) ([a6af4b3](https://github.com/27b-io/anthropic-lb/commit/a6af4b3c61535bfb259adcec8d14007925c75ede))
* correct Dockerfile CMD to use positional arg ([#12](https://github.com/27b-io/anthropic-lb/issues/12)) ([446e822](https://github.com/27b-io/anthropic-lb/commit/446e822c72bfec290a6d1a6a8610185d0a2f455c))
* eliminate budget check TOCTOU race ([#24](https://github.com/27b-io/anthropic-lb/issues/24)) ([2b42cc5](https://github.com/27b-io/anthropic-lb/commit/2b42cc508c6244110d08983e4500e0bd576a1266))
* enable affinity override for 3+ candidates in StickyWeightedV2 ([#40](https://github.com/27b-io/anthropic-lb/issues/40)) ([4079b70](https://github.com/27b-io/anthropic-lb/commit/4079b70e53b8eed444b485a8393acc2187c77106))
* graceful drain + SSE error frames on upstream interruption ([#62](https://github.com/27b-io/anthropic-lb/issues/62)) ([256027a](https://github.com/27b-io/anthropic-lb/commit/256027af96253ccd703805a22ec8f7d133abced2))
* inject OAuth system prompt for sonnet/opus model access ([#32](https://github.com/27b-io/anthropic-lb/issues/32)) ([790f7c8](https://github.com/27b-io/anthropic-lb/commit/790f7c8f2796f03d79d6bc2927d7892244e1d3c7))
* **LAB-1639:** make the redis startup connect background + retrying ([#129](https://github.com/27b-io/anthropic-lb/issues/129)) ([02a732e](https://github.com/27b-io/anthropic-lb/commit/02a732e45b8027455ffe6eb3f449c463fd1c3ef3))
* **LAB-1675:** unbrick the release pipeline — rustls, vX.Y.Z tags, decoupled publish ([#133](https://github.com/27b-io/anthropic-lb/issues/133)) ([970c53c](https://github.com/27b-io/anthropic-lb/commit/970c53c7bcb646875b933df4acd9b93aa3337953))
* log stream completion when usage is empty ([#38](https://github.com/27b-io/anthropic-lb/issues/38)) ([f85f141](https://github.com/27b-io/anthropic-lb/commit/f85f1412b1a435de6d323032051e960b9e843c0d))
* logging, utilization fail-open, and emergency brake toggle ([#34](https://github.com/27b-io/anthropic-lb/issues/34)) ([7cf8ebc](https://github.com/27b-io/anthropic-lb/commit/7cf8ebcd94d784e2cb69cc6733a998c36fe6a966))
* non-streaming requests bypass the SSE-tuned read_timeout (LAB-718) ([#102](https://github.com/27b-io/anthropic-lb/issues/102)) ([18ee187](https://github.com/27b-io/anthropic-lb/commit/18ee187a37b328e68db39eac30ddd6cea9ed27ec))
* override affinity routing when weight disparity is large ([#28](https://github.com/27b-io/anthropic-lb/issues/28)) ([3a42187](https://github.com/27b-io/anthropic-lb/commit/3a42187b1b0dc99df1701c1974d7c0bdbad1f130))
* preserve_order to prevent prompt cache corruption ([#44](https://github.com/27b-io/anthropic-lb/issues/44)) ([c5461bd](https://github.com/27b-io/anthropic-lb/commit/c5461bdf4e43249749477e8466f18a574f335aa8))
* retry loop skips already-tried accounts on 429/529 ([#26](https://github.com/27b-io/anthropic-lb/issues/26)) ([92fd6a4](https://github.com/27b-io/anthropic-lb/commit/92fd6a427f01506eb2a7928ecaa025afc7940d8d))
* round util_5h/util_7d to 2 decimal places in logs ([#35](https://github.com/27b-io/anthropic-lb/issues/35)) ([01c7f98](https://github.com/27b-io/anthropic-lb/commit/01c7f98827216e29dfb565c85d106ec4ba343333))
* scan all system blocks for OAuth prompt to prevent cache-breaking re-serialization ([#57](https://github.com/27b-io/anthropic-lb/issues/57)) ([e25ec68](https://github.com/27b-io/anthropic-lb/commit/e25ec683af869cdfbbe6d3cca535df89d6ea447b))
* session-stable affinity migration (stop cascade + cache-creation churn) ([#80](https://github.com/27b-io/anthropic-lb/issues/80)) ([92f0a7b](https://github.com/27b-io/anthropic-lb/commit/92f0a7b30d89b8f15bb7ff9dd80026956a1ca8e8))
* skip OAuth re-serialization when client includes CC system prompt ([#45](https://github.com/27b-io/anthropic-lb/issues/45)) ([1504e18](https://github.com/27b-io/anthropic-lb/commit/1504e18cd7aefac6a8a55a6d96aa645b336c4b3d))
* skip reset_seconds emission for stale epochs instead of emitting 0.0 ([#52](https://github.com/27b-io/anthropic-lb/issues/52)) ([ec8f88d](https://github.com/27b-io/anthropic-lb/commit/ec8f88db90258b4ef3276a38542b2695a0ea0b27))
* strip accept-encoding in proxy_handler to prevent gzip SSE ([#56](https://github.com/27b-io/anthropic-lb/issues/56)) ([a65bcf9](https://github.com/27b-io/anthropic-lb/commit/a65bcf9011b6bf7a224084ff35f416a3ba663872))
* surface upstream body-read failures; warn on broken affinity ([#63](https://github.com/27b-io/anthropic-lb/issues/63)) ([88f9d6c](https://github.com/27b-io/anthropic-lb/commit/88f9d6c698dd1a706b1450e7ec4d0892aac50001))

## [0.2.2](https://github.com/27b-io/anthropic-lb/compare/anthropic-lb-v0.2.1...anthropic-lb-v0.2.2) (2026-08-08)


### Features

* add /metrics Prometheus endpoint ([#19](https://github.com/27b-io/anthropic-lb/issues/19)) ([b5c4b87](https://github.com/27b-io/anthropic-lb/commit/b5c4b8733b674bf5963fdde9298cd444ca8f59e2))
* add rate-limit status, effective gate, and data age Prometheus metrics ([#53](https://github.com/27b-io/anthropic-lb/issues/53)) ([400e7e0](https://github.com/27b-io/anthropic-lb/commit/400e7e05ad2ff015a542c1ba687d130313394507))
* add reset countdown and account-level waste risk Prometheus metrics ([#48](https://github.com/27b-io/anthropic-lb/issues/48)) ([c1ed8e3](https://github.com/27b-io/anthropic-lb/commit/c1ed8e3ab9e8180986d7e500745c0916ebe5a877))
* add tool calling to OpenAI compat layer ([#22](https://github.com/27b-io/anthropic-lb/issues/22)) ([02ae5e7](https://github.com/27b-io/anthropic-lb/commit/02ae5e7839acb99ccfebcd1240937db969d524a7))
* distributed state via Redis ([#13](https://github.com/27b-io/anthropic-lb/issues/13)) ([2779092](https://github.com/27b-io/anthropic-lb/commit/2779092275360f3366d08dc21c09df23aef547b6))
* fold content fingerprint into the affinity key (distribute fan-outs) ([#83](https://github.com/27b-io/anthropic-lb/issues/83)) ([82be81f](https://github.com/27b-io/anthropic-lb/commit/82be81f906114556d41af956420a2c0253b7fb88))
* log probe weights and deduplicate probes across pods ([#43](https://github.com/27b-io/anthropic-lb/issues/43)) ([35082e3](https://github.com/27b-io/anthropic-lb/commit/35082e32d5fbdf391b1e5fc126ac75469eedb549))
* overage-aware routing with unified endpoint priority ([#59](https://github.com/27b-io/anthropic-lb/issues/59)) ([bde8f1f](https://github.com/27b-io/anthropic-lb/commit/bde8f1f3e61d4d627efd51b02b31adfa5d0bec4f))
* priority tiers and upstream fallback ([#58](https://github.com/27b-io/anthropic-lb/issues/58)) ([d94aec8](https://github.com/27b-io/anthropic-lb/commit/d94aec894281b6b152e771a2e18353419732c7f6))
* support multiple operator client IDs ([#14](https://github.com/27b-io/anthropic-lb/issues/14)) ([5ba63d3](https://github.com/27b-io/anthropic-lb/commit/5ba63d3461d3665342a29ccd73df53d423d6dbc5))
* token budget backpressure with per-claim 7d routing ([#10](https://github.com/27b-io/anthropic-lb/issues/10)) ([e313e6e](https://github.com/27b-io/anthropic-lb/commit/e313e6e594433b7e95836e3470f643d6355ed18f))
* transport circuit-breaker for persistently-dead endpoints ([#70](https://github.com/27b-io/anthropic-lb/issues/70)) ([#89](https://github.com/27b-io/anthropic-lb/issues/89)) ([1c3e6d9](https://github.com/27b-io/anthropic-lb/commit/1c3e6d965b5313570ec3a89c8b5c2886e8ffcc71))
* unified endpoints — collapse accounts + upstreams into one schema ([#61](https://github.com/27b-io/anthropic-lb/issues/61)) ([562420f](https://github.com/27b-io/anthropic-lb/commit/562420fcb24ad42e88b85df9ed0fc6e5e77b32bd))
* upstream transient-failure resilience (round-gated retry, 503, transport-error metric) ([#72](https://github.com/27b-io/anthropic-lb/issues/72)) ([742fc15](https://github.com/27b-io/anthropic-lb/commit/742fc1580458c661e921072a5fd86f1b9bbc04c3))
* waste-risk routing for 7d quota maximization ([#16](https://github.com/27b-io/anthropic-lb/issues/16)) ([3fb4fb9](https://github.com/27b-io/anthropic-lb/commit/3fb4fb9cd1431ae6040816cc595470cbcbf03a49))


### Bug Fixes

* 429 recovery and burst rate-limit detection ([#17](https://github.com/27b-io/anthropic-lb/issues/17)) ([4867b59](https://github.com/27b-io/anthropic-lb/commit/4867b59a71ba5ffe667896f4214efbdee3b89abb))
* add request-balance override for affinity with few clients ([#30](https://github.com/27b-io/anthropic-lb/issues/30)) ([24047d9](https://github.com/27b-io/anthropic-lb/commit/24047d9ab7d1eb825327ffaac4adaa9260914706))
* address bug hunt findings ([#37](https://github.com/27b-io/anthropic-lb/issues/37)) ([d333e7e](https://github.com/27b-io/anthropic-lb/commit/d333e7ec87b4f57f35522f6eaa1c03aa648cdc20))
* BEBO retry on 529 overloaded responses ([#31](https://github.com/27b-io/anthropic-lb/issues/31)) ([5ebfb6f](https://github.com/27b-io/anthropic-lb/commit/5ebfb6f72d9848215b164fee78cfa93901d0bf93))
* clamp legacy utilization fallback, correct stale defaults ([#15](https://github.com/27b-io/anthropic-lb/issues/15)) ([89b4231](https://github.com/27b-io/anthropic-lb/commit/89b42316777947fe53094b9d199b262e4d640f79))
* close idle inbound keep-alive conns to avoid client socket-reuse races ([#65](https://github.com/27b-io/anthropic-lb/issues/65)) ([a6af4b3](https://github.com/27b-io/anthropic-lb/commit/a6af4b3c61535bfb259adcec8d14007925c75ede))
* correct Dockerfile CMD to use positional arg ([#12](https://github.com/27b-io/anthropic-lb/issues/12)) ([446e822](https://github.com/27b-io/anthropic-lb/commit/446e822c72bfec290a6d1a6a8610185d0a2f455c))
* eliminate budget check TOCTOU race ([#24](https://github.com/27b-io/anthropic-lb/issues/24)) ([2b42cc5](https://github.com/27b-io/anthropic-lb/commit/2b42cc508c6244110d08983e4500e0bd576a1266))
* enable affinity override for 3+ candidates in StickyWeightedV2 ([#40](https://github.com/27b-io/anthropic-lb/issues/40)) ([4079b70](https://github.com/27b-io/anthropic-lb/commit/4079b70e53b8eed444b485a8393acc2187c77106))
* graceful drain + SSE error frames on upstream interruption ([#62](https://github.com/27b-io/anthropic-lb/issues/62)) ([256027a](https://github.com/27b-io/anthropic-lb/commit/256027af96253ccd703805a22ec8f7d133abced2))
* inject OAuth system prompt for sonnet/opus model access ([#32](https://github.com/27b-io/anthropic-lb/issues/32)) ([790f7c8](https://github.com/27b-io/anthropic-lb/commit/790f7c8f2796f03d79d6bc2927d7892244e1d3c7))
* **LAB-1639:** make the redis startup connect background + retrying ([#129](https://github.com/27b-io/anthropic-lb/issues/129)) ([02a732e](https://github.com/27b-io/anthropic-lb/commit/02a732e45b8027455ffe6eb3f449c463fd1c3ef3))
* log stream completion when usage is empty ([#38](https://github.com/27b-io/anthropic-lb/issues/38)) ([f85f141](https://github.com/27b-io/anthropic-lb/commit/f85f1412b1a435de6d323032051e960b9e843c0d))
* logging, utilization fail-open, and emergency brake toggle ([#34](https://github.com/27b-io/anthropic-lb/issues/34)) ([7cf8ebc](https://github.com/27b-io/anthropic-lb/commit/7cf8ebcd94d784e2cb69cc6733a998c36fe6a966))
* non-streaming requests bypass the SSE-tuned read_timeout (LAB-718) ([#102](https://github.com/27b-io/anthropic-lb/issues/102)) ([18ee187](https://github.com/27b-io/anthropic-lb/commit/18ee187a37b328e68db39eac30ddd6cea9ed27ec))
* override affinity routing when weight disparity is large ([#28](https://github.com/27b-io/anthropic-lb/issues/28)) ([3a42187](https://github.com/27b-io/anthropic-lb/commit/3a42187b1b0dc99df1701c1974d7c0bdbad1f130))
* preserve_order to prevent prompt cache corruption ([#44](https://github.com/27b-io/anthropic-lb/issues/44)) ([c5461bd](https://github.com/27b-io/anthropic-lb/commit/c5461bdf4e43249749477e8466f18a574f335aa8))
* retry loop skips already-tried accounts on 429/529 ([#26](https://github.com/27b-io/anthropic-lb/issues/26)) ([92fd6a4](https://github.com/27b-io/anthropic-lb/commit/92fd6a427f01506eb2a7928ecaa025afc7940d8d))
* round util_5h/util_7d to 2 decimal places in logs ([#35](https://github.com/27b-io/anthropic-lb/issues/35)) ([01c7f98](https://github.com/27b-io/anthropic-lb/commit/01c7f98827216e29dfb565c85d106ec4ba343333))
* scan all system blocks for OAuth prompt to prevent cache-breaking re-serialization ([#57](https://github.com/27b-io/anthropic-lb/issues/57)) ([e25ec68](https://github.com/27b-io/anthropic-lb/commit/e25ec683af869cdfbbe6d3cca535df89d6ea447b))
* session-stable affinity migration (stop cascade + cache-creation churn) ([#80](https://github.com/27b-io/anthropic-lb/issues/80)) ([92f0a7b](https://github.com/27b-io/anthropic-lb/commit/92f0a7b30d89b8f15bb7ff9dd80026956a1ca8e8))
* skip OAuth re-serialization when client includes CC system prompt ([#45](https://github.com/27b-io/anthropic-lb/issues/45)) ([1504e18](https://github.com/27b-io/anthropic-lb/commit/1504e18cd7aefac6a8a55a6d96aa645b336c4b3d))
* skip reset_seconds emission for stale epochs instead of emitting 0.0 ([#52](https://github.com/27b-io/anthropic-lb/issues/52)) ([ec8f88d](https://github.com/27b-io/anthropic-lb/commit/ec8f88db90258b4ef3276a38542b2695a0ea0b27))
* strip accept-encoding in proxy_handler to prevent gzip SSE ([#56](https://github.com/27b-io/anthropic-lb/issues/56)) ([a65bcf9](https://github.com/27b-io/anthropic-lb/commit/a65bcf9011b6bf7a224084ff35f416a3ba663872))
* surface upstream body-read failures; warn on broken affinity ([#63](https://github.com/27b-io/anthropic-lb/issues/63)) ([88f9d6c](https://github.com/27b-io/anthropic-lb/commit/88f9d6c698dd1a706b1450e7ec4d0892aac50001))

## [0.2.1](https://github.com/27b-io/anthropic-lb/compare/anthropic-lb-v0.2.0...anthropic-lb-v0.2.1) (2026-08-07)


### Features

* add /metrics Prometheus endpoint ([#19](https://github.com/27b-io/anthropic-lb/issues/19)) ([b5c4b87](https://github.com/27b-io/anthropic-lb/commit/b5c4b8733b674bf5963fdde9298cd444ca8f59e2))
* add rate-limit status, effective gate, and data age Prometheus metrics ([#53](https://github.com/27b-io/anthropic-lb/issues/53)) ([400e7e0](https://github.com/27b-io/anthropic-lb/commit/400e7e05ad2ff015a542c1ba687d130313394507))
* add reset countdown and account-level waste risk Prometheus metrics ([#48](https://github.com/27b-io/anthropic-lb/issues/48)) ([c1ed8e3](https://github.com/27b-io/anthropic-lb/commit/c1ed8e3ab9e8180986d7e500745c0916ebe5a877))
* add tool calling to OpenAI compat layer ([#22](https://github.com/27b-io/anthropic-lb/issues/22)) ([02ae5e7](https://github.com/27b-io/anthropic-lb/commit/02ae5e7839acb99ccfebcd1240937db969d524a7))
* distributed state via Redis ([#13](https://github.com/27b-io/anthropic-lb/issues/13)) ([2779092](https://github.com/27b-io/anthropic-lb/commit/2779092275360f3366d08dc21c09df23aef547b6))
* fold content fingerprint into the affinity key (distribute fan-outs) ([#83](https://github.com/27b-io/anthropic-lb/issues/83)) ([82be81f](https://github.com/27b-io/anthropic-lb/commit/82be81f906114556d41af956420a2c0253b7fb88))
* log probe weights and deduplicate probes across pods ([#43](https://github.com/27b-io/anthropic-lb/issues/43)) ([35082e3](https://github.com/27b-io/anthropic-lb/commit/35082e32d5fbdf391b1e5fc126ac75469eedb549))
* overage-aware routing with unified endpoint priority ([#59](https://github.com/27b-io/anthropic-lb/issues/59)) ([bde8f1f](https://github.com/27b-io/anthropic-lb/commit/bde8f1f3e61d4d627efd51b02b31adfa5d0bec4f))
* priority tiers and upstream fallback ([#58](https://github.com/27b-io/anthropic-lb/issues/58)) ([d94aec8](https://github.com/27b-io/anthropic-lb/commit/d94aec894281b6b152e771a2e18353419732c7f6))
* support multiple operator client IDs ([#14](https://github.com/27b-io/anthropic-lb/issues/14)) ([5ba63d3](https://github.com/27b-io/anthropic-lb/commit/5ba63d3461d3665342a29ccd73df53d423d6dbc5))
* token budget backpressure with per-claim 7d routing ([#10](https://github.com/27b-io/anthropic-lb/issues/10)) ([e313e6e](https://github.com/27b-io/anthropic-lb/commit/e313e6e594433b7e95836e3470f643d6355ed18f))
* transport circuit-breaker for persistently-dead endpoints ([#70](https://github.com/27b-io/anthropic-lb/issues/70)) ([#89](https://github.com/27b-io/anthropic-lb/issues/89)) ([1c3e6d9](https://github.com/27b-io/anthropic-lb/commit/1c3e6d965b5313570ec3a89c8b5c2886e8ffcc71))
* unified endpoints — collapse accounts + upstreams into one schema ([#61](https://github.com/27b-io/anthropic-lb/issues/61)) ([562420f](https://github.com/27b-io/anthropic-lb/commit/562420fcb24ad42e88b85df9ed0fc6e5e77b32bd))
* upstream transient-failure resilience (round-gated retry, 503, transport-error metric) ([#72](https://github.com/27b-io/anthropic-lb/issues/72)) ([742fc15](https://github.com/27b-io/anthropic-lb/commit/742fc1580458c661e921072a5fd86f1b9bbc04c3))
* waste-risk routing for 7d quota maximization ([#16](https://github.com/27b-io/anthropic-lb/issues/16)) ([3fb4fb9](https://github.com/27b-io/anthropic-lb/commit/3fb4fb9cd1431ae6040816cc595470cbcbf03a49))


### Bug Fixes

* 429 recovery and burst rate-limit detection ([#17](https://github.com/27b-io/anthropic-lb/issues/17)) ([4867b59](https://github.com/27b-io/anthropic-lb/commit/4867b59a71ba5ffe667896f4214efbdee3b89abb))
* add request-balance override for affinity with few clients ([#30](https://github.com/27b-io/anthropic-lb/issues/30)) ([24047d9](https://github.com/27b-io/anthropic-lb/commit/24047d9ab7d1eb825327ffaac4adaa9260914706))
* address bug hunt findings ([#37](https://github.com/27b-io/anthropic-lb/issues/37)) ([d333e7e](https://github.com/27b-io/anthropic-lb/commit/d333e7ec87b4f57f35522f6eaa1c03aa648cdc20))
* BEBO retry on 529 overloaded responses ([#31](https://github.com/27b-io/anthropic-lb/issues/31)) ([5ebfb6f](https://github.com/27b-io/anthropic-lb/commit/5ebfb6f72d9848215b164fee78cfa93901d0bf93))
* clamp legacy utilization fallback, correct stale defaults ([#15](https://github.com/27b-io/anthropic-lb/issues/15)) ([89b4231](https://github.com/27b-io/anthropic-lb/commit/89b42316777947fe53094b9d199b262e4d640f79))
* close idle inbound keep-alive conns to avoid client socket-reuse races ([#65](https://github.com/27b-io/anthropic-lb/issues/65)) ([a6af4b3](https://github.com/27b-io/anthropic-lb/commit/a6af4b3c61535bfb259adcec8d14007925c75ede))
* correct Dockerfile CMD to use positional arg ([#12](https://github.com/27b-io/anthropic-lb/issues/12)) ([446e822](https://github.com/27b-io/anthropic-lb/commit/446e822c72bfec290a6d1a6a8610185d0a2f455c))
* eliminate budget check TOCTOU race ([#24](https://github.com/27b-io/anthropic-lb/issues/24)) ([2b42cc5](https://github.com/27b-io/anthropic-lb/commit/2b42cc508c6244110d08983e4500e0bd576a1266))
* enable affinity override for 3+ candidates in StickyWeightedV2 ([#40](https://github.com/27b-io/anthropic-lb/issues/40)) ([4079b70](https://github.com/27b-io/anthropic-lb/commit/4079b70e53b8eed444b485a8393acc2187c77106))
* graceful drain + SSE error frames on upstream interruption ([#62](https://github.com/27b-io/anthropic-lb/issues/62)) ([256027a](https://github.com/27b-io/anthropic-lb/commit/256027af96253ccd703805a22ec8f7d133abced2))
* inject OAuth system prompt for sonnet/opus model access ([#32](https://github.com/27b-io/anthropic-lb/issues/32)) ([790f7c8](https://github.com/27b-io/anthropic-lb/commit/790f7c8f2796f03d79d6bc2927d7892244e1d3c7))
* **LAB-1639:** make the redis startup connect background + retrying ([#129](https://github.com/27b-io/anthropic-lb/issues/129)) ([02a732e](https://github.com/27b-io/anthropic-lb/commit/02a732e45b8027455ffe6eb3f449c463fd1c3ef3))
* log stream completion when usage is empty ([#38](https://github.com/27b-io/anthropic-lb/issues/38)) ([f85f141](https://github.com/27b-io/anthropic-lb/commit/f85f1412b1a435de6d323032051e960b9e843c0d))
* logging, utilization fail-open, and emergency brake toggle ([#34](https://github.com/27b-io/anthropic-lb/issues/34)) ([7cf8ebc](https://github.com/27b-io/anthropic-lb/commit/7cf8ebcd94d784e2cb69cc6733a998c36fe6a966))
* non-streaming requests bypass the SSE-tuned read_timeout (LAB-718) ([#102](https://github.com/27b-io/anthropic-lb/issues/102)) ([18ee187](https://github.com/27b-io/anthropic-lb/commit/18ee187a37b328e68db39eac30ddd6cea9ed27ec))
* override affinity routing when weight disparity is large ([#28](https://github.com/27b-io/anthropic-lb/issues/28)) ([3a42187](https://github.com/27b-io/anthropic-lb/commit/3a42187b1b0dc99df1701c1974d7c0bdbad1f130))
* preserve_order to prevent prompt cache corruption ([#44](https://github.com/27b-io/anthropic-lb/issues/44)) ([c5461bd](https://github.com/27b-io/anthropic-lb/commit/c5461bdf4e43249749477e8466f18a574f335aa8))
* retry loop skips already-tried accounts on 429/529 ([#26](https://github.com/27b-io/anthropic-lb/issues/26)) ([92fd6a4](https://github.com/27b-io/anthropic-lb/commit/92fd6a427f01506eb2a7928ecaa025afc7940d8d))
* round util_5h/util_7d to 2 decimal places in logs ([#35](https://github.com/27b-io/anthropic-lb/issues/35)) ([01c7f98](https://github.com/27b-io/anthropic-lb/commit/01c7f98827216e29dfb565c85d106ec4ba343333))
* scan all system blocks for OAuth prompt to prevent cache-breaking re-serialization ([#57](https://github.com/27b-io/anthropic-lb/issues/57)) ([e25ec68](https://github.com/27b-io/anthropic-lb/commit/e25ec683af869cdfbbe6d3cca535df89d6ea447b))
* session-stable affinity migration (stop cascade + cache-creation churn) ([#80](https://github.com/27b-io/anthropic-lb/issues/80)) ([92f0a7b](https://github.com/27b-io/anthropic-lb/commit/92f0a7b30d89b8f15bb7ff9dd80026956a1ca8e8))
* skip OAuth re-serialization when client includes CC system prompt ([#45](https://github.com/27b-io/anthropic-lb/issues/45)) ([1504e18](https://github.com/27b-io/anthropic-lb/commit/1504e18cd7aefac6a8a55a6d96aa645b336c4b3d))
* skip reset_seconds emission for stale epochs instead of emitting 0.0 ([#52](https://github.com/27b-io/anthropic-lb/issues/52)) ([ec8f88d](https://github.com/27b-io/anthropic-lb/commit/ec8f88db90258b4ef3276a38542b2695a0ea0b27))
* strip accept-encoding in proxy_handler to prevent gzip SSE ([#56](https://github.com/27b-io/anthropic-lb/issues/56)) ([a65bcf9](https://github.com/27b-io/anthropic-lb/commit/a65bcf9011b6bf7a224084ff35f416a3ba663872))
* surface upstream body-read failures; warn on broken affinity ([#63](https://github.com/27b-io/anthropic-lb/issues/63)) ([88f9d6c](https://github.com/27b-io/anthropic-lb/commit/88f9d6c698dd1a706b1450e7ec4d0892aac50001))
