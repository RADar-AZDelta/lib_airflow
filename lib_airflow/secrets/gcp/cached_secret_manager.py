from typing import Optional

from airflow.providers.google.cloud.secrets.secret_manager import (
    CloudSecretManagerBackend,
)


class CachedCloudSecretManagerBackend(CloudSecretManagerBackend):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._cached_secret = {}

    def _get_secret(self, path_prefix: str, secret_id: str) -> Optional[str]:
        key = self.build_path(path_prefix, secret_id, self.sep)

        if not key in self._cached_secret:
            self._cached_secret[key] = super()._get_secret(path_prefix, secret_id)

        return self._cached_secret[key]
