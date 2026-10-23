import { defineConfig } from 'vitepress'

export default defineConfig({
  base: '/shard/',
  title: "Shard",
  description: "Entity sharding for Scala",
  head: [
    ['link', { rel: 'icon', href: '/shard/favicon.png' }],
    ['script', { defer: '', src: 'https://cloud.umami.is/script.js', 'data-website-id': '2ba71608-2c44-4547-804e-ed78f3a41ad1' }]
  ],

  themeConfig: {
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Documentation', link: '/getting-started' },
      { text: 'FAQ', link: '/faq' },
      { text: 'About', link: '/about' },
    ],

    logo: '/shard.svg',

    sidebar: [
      {
        text: 'Documentation',
        items: [
          { text: 'Getting started', link: '/getting-started' },
          { text: 'Entities and messaging', link: '/entities' },
          { text: 'Architecture', link: '/architecture' },
          { text: 'Configuration', link: '/config' },
          { text: 'Customization', link: '/customization' },
          { text: 'Metrics', link: '/metrics' },
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
