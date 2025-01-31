import {
  Alert,
  Box,
  DialogActions,
  DialogContent,
  Snackbar,
  Typography,
} from '@mui/material';
import Button from '@mui/material/Button';
import Dialog from '@mui/material/Dialog';
import DialogTitle from '@mui/material/DialogTitle';
import { useRouter } from 'next/router';
import React, { useEffect, useState } from 'react';

import UserProfile from '../../../utils/auth';
import {
  connectIntegration,
  formatService,
  IntegrationConfig,
  Service,
  SupportedIntegrations,
} from '../../../utils/integrations';
import { BigQueryDialog } from './bigqueryDialog';
import { IntegrationTextInputField } from './IntegrationTextInputField';
import { MariaDbDialog } from './mariadbDialog';
import { MysqlDialog } from './mysqlDialog';
import { PostgresDialog } from './postgresDialog';
import { RedshiftDialog } from './redshiftDialog';
import { S3Dialog } from './s3Dialog';
import { SnowflakeDialog } from './snowflakeDialog';

type Props = {
  user: UserProfile;
  service: Service;
  onCloseDialog: () => void;
};

export const IntegrationDialog: React.FC<Props> = ({
  user,
  service,
  onCloseDialog,
}) => {
  const router = useRouter();
  const [config, setConfig] = useState<IntegrationConfig>({});
  const [disableConnect, setDisableConnect] = useState(true);
  const [successMessage, setSuccessMessage] = useState('');
  const [showSuccessToast, setShowSuccessToast] = useState(false);

  const namePrefix = formatService(service) + '/';
  const [name, setName] = useState(namePrefix);

  const handleSuccessToastClose = () => {
    setShowSuccessToast(false);
  };

  useEffect(() => {
    setDisableConnect(
      service !== 'Aqueduct Demo' &&
        (!isConfigComplete(config) || name === '' || name === namePrefix)
    );
  }, [config, name]);

  const dialogHeader = (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'row',
        justifyContent: 'space-between',
        width: '100%',
      }}
    >
      <Typography variant="h5">{'Connect to ' + service}</Typography>
      <img height="45px" src={SupportedIntegrations[service].logo} />
    </Box>
  );

  let serviceDialog;

  switch (service) {
    case 'Postgres':
      serviceDialog = <PostgresDialog setDialogConfig={setConfig} />;
      break;
    case 'Snowflake':
      serviceDialog = <SnowflakeDialog setDialogConfig={setConfig} />;
      break;
    case 'Aqueduct Demo':
      serviceDialog = null;
      break;
    case 'MySQL':
      serviceDialog = <MysqlDialog setDialogConfig={setConfig} />;
      break;
    case 'Redshift':
      serviceDialog = <RedshiftDialog setDialogConfig={setConfig} />;
      break;
    case 'MariaDB':
      serviceDialog = <MariaDbDialog setDialogConfig={setConfig} />;
      break;
    case 'BigQuery':
      serviceDialog = <BigQueryDialog setDialogConfig={setConfig} />;
      break;
    case 'S3':
      serviceDialog = <S3Dialog setDialogConfig={setConfig} />;
      break;
    default:
      return null;
  }

  const confirmConnect = () => {
    setIsConnecting(true);
    setErrMsg(null);
    connectIntegration(user, service, name, config)
      .then(() => {
        setShowSuccessToast(true);
        setSuccessMessage('Successfully connected to ' + service + '!');
        setIsConnecting(false);
        onCloseDialog();
        router.push('/integrations');
      })
      .catch((err) => {
        setErrMsg('Unable to connect integration. ' + err.message);
        setIsConnecting(false);
      });
  };

  const nameInput = (
    <IntegrationTextInputField
      spellCheck={false}
      required={true}
      label="Name*"
      description="Provide a unique name to refer to this integration."
      placeholder={namePrefix}
      onChange={(event) => {
        const input = event.target.value;
        event.target.value = namePrefix + input.substr(namePrefix.length);
        setName(event.target.value);
      }}
      value={name}
      disabled={service === 'Aqueduct Demo'}
    />
  );

  const [isConnecting, setIsConnecting] = useState(false);
  const [errMsg, setErrMsg] = useState(null);

  return (
    <Dialog open={true} onClose={onCloseDialog}>
      <DialogTitle>{dialogHeader}</DialogTitle>
      <DialogContent>
        {nameInput}
        {serviceDialog}
        {errMsg && <Alert severity="error">{errMsg}</Alert>}
        <Snackbar
          anchorOrigin={{ vertical: 'top', horizontal: 'center' }}
          open={showSuccessToast}
          onClose={handleSuccessToastClose}
          key={'integrations-dialog-success-snackbar'}
          autoHideDuration={6000}
        >
          <Alert
            onClose={handleSuccessToastClose}
            severity="success"
            sx={{ width: '100%' }}
          >
            {successMessage}
          </Alert>
        </Snackbar>
      </DialogContent>
      <DialogActions>
        <Button autoFocus onClick={onCloseDialog}>
          Cancel
        </Button>
        <Button
          autoFocus
          onClick={confirmConnect}
          disabled={isConnecting || disableConnect}
        >
          Confirm
        </Button>
      </DialogActions>
    </Dialog>
  );
};

// Helper function to check if the Integration config is completely filled
function isConfigComplete(config: IntegrationConfig): boolean {
  return Object.values(config).every((x) => x && x !== '');
}
