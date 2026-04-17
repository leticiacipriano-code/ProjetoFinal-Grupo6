FROM python:3.12-slim

# Download the latest installer
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

WORKDIR /app

# Copy the project into the image
COPY . .

# Guarantees permissions
RUN chmod +x ./entrypoint.sh

# Disable development dependencies
ENV UV_NO_DEV=1

# Sync the project into a new environment, asserting the lockfile is up to date
RUN uv --version
RUN uv sync --locked

# Presuming there is a `my_app` command provided by the project
RUN sed -i 's/\r$//' entrypoint.sh
ENTRYPOINT [ "./entrypoint.sh" ]