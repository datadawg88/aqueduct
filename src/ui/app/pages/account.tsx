import { DefaultLayout, useAqueductConsts, useUser } from '@aqueducthq/common';
import { Box, Typography } from '@mui/material';
import Head from 'next/head';
import { useRouter } from 'next/router';
import React from 'react';

export { getServerSideProps } from '@aqueducthq/common';

const Account: React.FC = () => {
    const router = useRouter();
    const { user, loading, success } = useUser();
    const { apiAddress } = useAqueductConsts();
    const serverAddress = apiAddress ? `${apiAddress}` : '<server address>';
    const apiConnectionSnippet = `import aqueduct
client = aqueduct.Client(
    "${user.apiKey}",
    "${serverAddress}"
)`;
    const maxContentWidth = '600px';

    if (loading) {
        return null;
    }

    if (!success) {
        router.push('/login');
        return null;
    }

    return (
        <DefaultLayout user={user}>
            <Head>
                <title>Account | Aqueduct</title>
            </Head>

            <Typography variant="h2" gutterBottom component="div">
                Account Overview
            </Typography>

            <Typography variant="h5" sx={{ mt: 3 }}>
                API Key
            </Typography>
            <Box sx={{ my: 1 }}>
                <code>{user.apiKey}</code>
            </Box>

            <Box
                sx={{
                    display: 'flex',
                    flexDirection: 'column',
                    width: maxContentWidth,
                }}
            >
                <Typography variant="body1" sx={{ fontWeight: 'bold', mr: '8px' }}>
                    Python SDK Connection Snippet
                </Typography>
                <Typography variant="body1" sx={{ fontFamily: 'monospace' }}>
                    {apiConnectionSnippet}
                </Typography>
            </Box>
        </DefaultLayout>
    );
};

export default Account;
