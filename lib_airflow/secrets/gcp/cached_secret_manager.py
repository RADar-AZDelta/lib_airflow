# Copyright 2022 RADar-AZDelta
# SPDX-License-Identifier: gpl3+

from typing import Optional

from airflow.providers.google.cloud.secrets.secret_manager import (
    CloudSecretManagerBackend,
)
from cachetools import TTLCache


class CachedCloudSecretManagerBackend(CloudSecretManagerBackend):
    """A caching variant of the CloudSecretManagerBackend.
    It caches the secrets retrieved from Secrets Manager, reducing the number of requests to Secrets Manager"""

    def __init__(
        self, *args, maxsize=100000, ttl=600, **kwargs
    ) -> None:  # 10 minutes time to live
        super().__init__(*args, **kwargs)

        self._cache = TTLCache(maxsize=maxsize, ttl=ttl)

    def _get_secret(self, path_prefix: str, secret_id: str) -> Optional[str]:
        """Get secret value from the SecretManager based on prefix.

        Args:
            path_prefix (str): Prefix for the Path to get Secret
            secret_id (str): Secret Key

        Returns:
            Optional[str]: The secret
        """
        key = self.build_path(path_prefix, secret_id, self.sep)

        try:
            value = self._cache[key]
        except KeyError:
            value = super()._get_secret(path_prefix, secret_id)
            self._cache[key] = value

        return value
