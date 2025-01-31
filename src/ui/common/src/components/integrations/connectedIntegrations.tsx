import Box from '@mui/material/Box';
import Divider from '@mui/material/Divider';
import React, { useEffect } from 'react';
import { useDispatch, useSelector } from 'react-redux';

import { IntegrationCard } from '../../components/integrations/cards/card';
import { handleLoadIntegrations } from '../../reducers/integrations';
import { AppDispatch, RootState } from '../../stores/store';
import { UserProfile } from '../../utils/auth';
import { Card } from '../layouts/card';

type ConnectedIntegrationsProps = {
  user: UserProfile;
};

export const ConnectedIntegrations: React.FC<ConnectedIntegrationsProps> = ({
  user,
}) => {
  const dispatch: AppDispatch = useDispatch();

  useEffect(() => {
    dispatch(handleLoadIntegrations({ apiKey: user.apiKey }));
  }, []);

  const integrations = useSelector(
    (state: RootState) => state.integrationsReducer.integrations
  );
  if (!integrations) {
    return null;
  }

  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'flex-start',
        my: 1,
      }}
    >
      {[...integrations]
        .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))
        .map((integration, idx) => {
          return (
            <Box key={idx}>
              <Card>
                <IntegrationCard integration={integration} />
              </Card>

              {idx < integrations.length - 1 && <Divider />}
            </Box>
          );
        })}
    </Box>
  );
};
