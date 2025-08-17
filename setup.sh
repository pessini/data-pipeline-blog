#!/bin/bash

# Data Pipeline Setup Script
echo "🚀 Setting up Data Pipeline with Docker Compose"
echo "================================================"

# Check if Docker and Docker Compose are installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "✅ Created .env file. Please review and update the credentials before proceeding."
    echo ""
    echo "🔧 To customize your setup:"
    echo "   1. Edit the .env file with your preferred credentials"
    echo "   2. Run this script again to start the services"
    echo ""
    read -p "Press Enter to open .env file for editing (or Ctrl+C to exit)..."
    ${EDITOR:-nano} .env
fi

echo ""
echo "🏗️  Building and starting services..."
docker-compose up -d --build

echo ""
echo "⏳ Waiting for services to be ready..."
sleep 10

echo ""
echo "🔍 Checking service status..."
docker-compose ps

echo ""
echo "🎉 Setup complete! Your services are available at:"
echo ""
echo "📊 Streamlit Dashboard:  http://localhost:17113"
echo "🔄 Prefect UI:          http://localhost:17112"
echo "🗂️  MinIO Console:       http://localhost:17111"
echo "📡 MinIO API:           http://localhost:17110"
echo ""
echo "📖 For more information, see DOCKER_SETUP.md"
echo ""
echo "🛠️  Useful commands:"
echo "   View logs:           docker-compose logs -f"
echo "   Stop services:       docker-compose down"
echo "   Restart services:    docker-compose restart"
echo "   Clean up:            docker-compose down -v"
