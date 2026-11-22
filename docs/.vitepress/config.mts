import { defineConfig } from 'vitepress'

export default defineConfig({
  base: '/shard/',
  title: "Shard",
  description: "Entity sharding for Scala",
  head: [
    ['link', { rel: 'icon', href: '/shard/favicon.png' }],
    ['script', { defer: '', src: 'https://cloud.umami.is/script.js', 'data-website-id': '2ba71608-2c44-4547-804e-ed78f3a41ad1' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:url', content: 'https://ghostdogpr.github.io/shard/' }],
    ['meta', { property: 'og:title', content: 'Shard' }],
    ['meta', { property: 'og:description', content: 'Entity sharding for Scala' }],
    ['meta', { property: 'og:image', content: 'https://ghostdogpr.github.io/shard/og.png' }],
    ['meta', { property: 'og:image:width', content: '1200' }],
    ['meta', { property: 'og:image:height', content: '630' }],
    ['meta', { property: 'og:image:alt', content: 'Shard logo (a split blue hexagon with Scala bars) above the title Shard: Entity sharding for Scala' }],
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }]
  ],

  themeConfig: {
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Documentation', link: '/getting-started' },
      { text: 'FAQ', link: '/faq' },
      { text: 'About', link: '/about' },
      { text: 'Scaladoc', link: 'https://javadoc.io/doc/com.github.ghostdogpr/shard-core_3/' },
    ],

    logo: '/shard.svg',

    sidebar: [
      {
        text: 'Documentation',
        items: [
          { text: 'Getting started', link: '/getting-started' },
          { text: 'Entities and messaging', link: '/entities' },
          { text: 'Delivery guarantees', link: '/delivery-guarantees' },
          { text: 'Architecture', link: '/architecture' },
          { text: 'Configuration', link: '/config' },
          { text: 'Customization', link: '/customization' },
          { text: 'Metrics', link: '/metrics' },
          { text: 'Migrating from Shardcake', link: '/migration' },
        ]
      },
      {
        text: 'FAQ', link: '/faq'
      },
      {
        text: 'About', link: '/about'
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/ghostdogpr/shard' }
    ],

    search: {
      provider: 'local'
    }
  }
})
