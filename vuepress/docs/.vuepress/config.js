module.exports = {
    base: '/shard/',
    head: [
        ['link', {
            rel: 'icon',
            href: '/shard/favicon.png'
        }]
    ],
    locales: {
        '/': {
            lang: 'en-US',
            title: 'Shard',
            description: 'Entity Sharding library for Scala',
        }
    },
    themeConfig: {
        logo: '/shard.svg',
        locales: {
            '/': {
                selectText: 'Language',
                label: 'English',
                nav: [
                    {
                        text: 'Documentation',
                        link: '/docs/'
                    },
                    {
                        text: 'FAQ',
                        link: '/faq/'
                    },
                    {
                        text: 'About',
                        link: '/about/'
                    },
                    {
                        text: 'Github',
                        link: 'https://github.com/ghostdogpr/shard'
                    }
                ],
                sidebar: {
                    '/docs/': [{
                        title: 'Documentation',
                        collapsable: false,
                        sidebarDepth: 2,
                        children: [
                            '',
                            'architecture',
                            'config',
                            'customization',
                            'metrics'
                        ]
                    }]
                }
            }
        },
    }
}