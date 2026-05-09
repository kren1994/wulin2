(function () {
    'use strict';

    // Set this to your deployed worker URL in production.
    // Example: 'wss://your-worker-subdomain.workers.dev/ws-peer'
    //const DEFAULT_WORKER_ENDPOINT = '';
    const DEFAULT_WORKER_ENDPOINT = 'wss://wulin-ws-worker.kamrenju.workers.dev/ws-peer';

    const query = new URLSearchParams(window.location.search);
    const fromQuery = String(query.get('wsEndpoint') || '').trim();
    const fromStorage = String(window.localStorage.getItem('wulin:ws-endpoint') || '').trim();

    const endpoint = fromQuery || fromStorage || DEFAULT_WORKER_ENDPOINT;
    if (fromQuery) {
        try {
            window.localStorage.setItem('wulin:ws-endpoint', fromQuery);
        } catch (error) {
            // no-op
        }
    }

    if (endpoint) {
        window.WULIN_WS_ENDPOINT = endpoint;
    }
})();
