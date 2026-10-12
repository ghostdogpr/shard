---
layout: home

hero:
  name: "Shard"
  text: "Entity sharding\nfor Scala"
  tagline: Distribute entities across servers and reach them by ID, wherever they live
  image:
    src: /shard.svg
    alt: Shard
  actions:
    - theme: brand
      text: Getting Started
      link: /getting-started
    - theme: alt
      text: GitHub
      link: https://github.com/ghostdogpr/shard

features:
  - title: Single writer pattern
    details: Ensure that each entity exists in only one place at a time across all your servers.
  - title: Location transparency
    details: Communicate with your remote entities using their ID, regardless of their physical location.
  - title: Customization
    details: Bring your own storage, serialization, messaging protocol... or use the ones provided.
---
