'use strict';
module.exports = [
    {
        schema: 'v1',
        provider: 'cmu',
        frequency: 'hour',
        meta: {
            folderId: '1Mp_a-OyGGlk5tGkezYK41iZ2qybnrPzp'
        }
    },
    {
        schema: 'v1',
        provider: 'habitatmap',
        frequency: 'hour',
        meta: {
            url: 'http://aircasting.habitatmap.org'
        }
    },
    {
        schema: 'v1',
        provider: 'purpleair',
        frequency: 'hour',
        meta: {
            url: 'https://api.purpleair.com/'
        }
    }
];
