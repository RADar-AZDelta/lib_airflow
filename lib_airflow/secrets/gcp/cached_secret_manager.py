# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from typing import Optional

from airflow.providers.google.cloud.secrets.secret_manager import (
    CloudSecretManagerBackend,
)


class CachedCloudSecretManagerBackend(CloudSecretManagerBackend):
    """A caching variant of the CloudSecretManagerBackend.
    It caches the secrets retrieved from Secrets Manager, reducing the number of requests to Secrets Manager"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._cached_secret = {}

    def _get_secret(self, path_prefix: str, secret_id: str) -> Optional[str]:
        """Get secret value from the SecretManager based on prefix.

        Args:
            path_prefix (str): Prefix for the Path to get Secret
            secret_id (str): Secret Key

        Returns:
            Optional[str]: The secret
        """
        key = self.build_path(path_prefix, secret_id, self.sep)

        if not key in self._cached_secret:
            self._cached_secret[key] = super()._get_secret(path_prefix, secret_id)

        return self._cached_secret[key]
