# Use a slim version of Python
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the entrypoint script into the image
COPY entrypoint.sh /usr/local/bin/
# Make it executable
RUN chmod +x /usr/local/bin/entrypoint.sh

# For security, we will run commands as a non-root user.
RUN addgroup --system user && adduser --system --group user

# Change ownership of the /app directory to the non-root user
RUN chown user:user /app

# Switch to the non-root user
USER user

# Set the entrypoint for the container
ENTRYPOINT ["entrypoint.sh"]